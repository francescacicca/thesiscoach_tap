import os
import json
import logging
import telegram
from elasticsearch import Elasticsearch
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters, CommandHandler
from confluent_kafka import Producer


# Recupero token
TOKEN = os.getenv("TELEGRAM_TOKEN")


# Kafka
config = {'bootstrap.servers': "kafka:9092"}
producer = Producer(config)


# ElasticSearch (per /topics)
elase = Elasticsearch(["http://elasticsearch:9200"])


# -- FUNZIONAMENTO BOT --#

# L'utente può gestire il progresso rispetto a più argomenti e tesi
user_topics = {}


def handle_report(err, msg):
    if err is not None:
        print(f"Errore Kafka: {err}")
    else:
        print(f"Inviato a Kafka: {msg.topic()}")


# -- FUNZIONE comando START DEL BOT -- #
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    user_topics[user_id] = "generale"

    welcome = (
        "<b>Ciao!</b> Sono il tuo Coach personale, e ti aiuterò nella preparazione alla discussione della tesi 😁\n\n"
        "Per prima cosa, crea o seleziona l'argomento della tua tesi col comando /topic [nome]\n"
        "Poi, se vuoi, puoi mandarmi il <i>documento</i> in PDF di riferimento 📄\n"
        "Altrimenti, inviami direttamente una <i>registrazione vocale</i> 🎙️\n\n"
        "Pochi istanti dopo, riceverai la tua <b>analisi personalizzata</b> 📊\n"
        "Se vuoi monitorare i tuoi progressi, clicca <a href='http://localhost:5601'>QUI</a> (solo da PC)!"
    )

    await update.message.reply_text(welcome, parse_mode="HTML")


# Selezione topic
async def set_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if context.args:
        topic = " ".join(context.args).lower()
        user_topics[user_id] = topic
        await update.message.reply_text(f"Argomento impostato su: <b>{topic}</b>\nOra puoi inviarmi file e/o audio! :)", parse_mode="HTML")
    else:
        await update.message.reply_text(f"Devi specificare un argomento\nEsempio: '/topic tesi_triennale'")


# Elenco topic esistenti
async def topics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    query = {
        "size": 0,
        "query": { "term": { "user_id": user_id } },
        "aggs": {
            "unique_topics": {
                "terms": { "field": "topic.keyword" }
            }
        }
    }

    try:
        topics = elase.search(index='thesis-analysis', body=query)
        buckets = topics['aggregations']['unique_topics']['buckets']

        if not buckets:
            await update.message.reply_text("Non hai ancora creato nessun topic")
            return
        
        topics_list = "\n".join([f"- {b['key']}" for b in buckets])
        await update.message.reply_text(f"📚 <b>Topic esistenti:</b>\n{topics_list}", parse_mode="HTML")
    
    except Exception as e:
        print(f"Errore ELASTICSEARCH {e}")
        await update.message.reply_text("Errore nel recupero dei topic. Riprovare")



# Eliminazione topic
async def delete_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id

    if context.args:
        topic_to_delete = " ".join(context.args).lower()

        if user_topics.get(user_id) == topic_to_delete:
            user_topics[user_id] = "generale"


        try:
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"user_id": user_id}},
                            {"term": {"topic.keyword": topic_to_delete}}
                        ]
                    }
                },
                "_source": ["path"] # Per cancellare tutti i file salvati fisicamente per il topic da cancellare
            }

            result = elase.search(index='thesis-analysis', body=query, size=100)
            hits = result['hits']['hits']

            # Eliminazione file audio
            deleted_audio = 0
            for hit in hits:
                audio_path = hit['_source'].get('path')
                if audio_path and os.path.exists(audio_path):
                    os.remove(audio_path)
                    deleted_audio += 1

            # Eliminazione documento PDF dal disco
            pdf_path = f"/data/pdf/{user_id}_{topic_to_delete}.pdf"
            deleted_pdf = False
            if os.path.exists(pdf_path):
                os.remove(pdf_path)
                deleted_pdf = True


            # Eliminazione record ES relativo al documento PDF
            try:
                elase.delete(index='student-pdfs', id=f"{user_id}_{topic_to_delete}")
                print(f"Rimozione record da student-pdfs per {topic_to_delete} effettuata con successo")
            except Exception as e:
                print(f"Nessun record trovato in student-pdfs per {topic_to_delete}. Errore: {e}")


            # Eliminazione records su ElasticSearch
            deleting = elase.delete_by_query(index='thesis-analysis', body=query['query'])
            deleted_records = deleting.get('deleted', 0)
            
            await update.message.reply_text(f"<b>Topic {topic_to_delete} eliminato correttamente.</b>\nRimossi {deleted_records} record dal database\n{'1 Documento PDF rimosso' if deleted_pdf else 'Nessun documento rimosso'}\nRimossi {deleted_audio} file audio", parse_mode="HTML")
        

        except Exception as e:
            print(f"Errore cancellazione ELASTICSEARCH {e}")
            await update.message.reply_text(f"Errore nella cancellazione dei record del topic <b>{topic_to_delete}</b>. Riprovare", parse_mode="HTML")

    else:
        await update.message.reply_text("Specifica il topic da eliminare\nEsempio: '/cancella tesi_triennale'")






# -- FUNZIONE DI GESTIONE AUDIO -- #
async def handle_speech(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    current_topic = user_topics.get(user_id, "generale")

    speech = update.message.voice or update.message.audio
    file = await context.bot.get_file(speech.file_id)

    # Salvataggio in /data/audio
    path = f"/data/audio/{speech.file_id}.ogg"

    try: 
        await file.download_to_drive(path)

        # --> Kafka
        msg = {
                "user_id": user_id,
                "topic": current_topic,
                "type": "audio", 
                "path": path

            }
        producer.produce('tc-input', json.dumps(msg).encode('utf-8'), callback=handle_report)
        producer.flush()
        await update.message.reply_text(f"Audio ricevuto correttamente al topic: <b>{current_topic}</b>", parse_mode="HTML")

    except telegram.error.TimedOut:
        await update.message.reply_text("⚠️ Telegram ci sta mettendo troppo ad ascoltare il tuo messaggio. Prova a inviare l'audio di nuovo!")




# -- FUNZIONE DI GESTIONE PDF -- #
async def handle_pdf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    current_topic = user_topics.get(user_id, "generale")

    doc = update.message.document
    if not doc.file_name.lower().endswith('.pdf'):
        await update.message.reply_text("Devi caricare un file in formato PDF")
        return
    
    file = await context.bot.get_file(doc.file_id)

    # Salvataggio in /data/pdf
    file_renamed = f"{user_id}_{current_topic}.pdf"
    path = f"/data/pdf/{file_renamed}"
    
    try:

        await file.download_to_drive(path)

        # --> Kafka
        msg = {
                "user_id": user_id,
                "topic": current_topic, 
                "type": "pdf", 
                "path": path
            }
        producer.produce('tc-input', json.dumps(msg).encode('utf-8'), callback=handle_report)
        producer.flush()
        await update.message.reply_text(f"PDF '{doc.file_name}' ricevuto correttamente al topic: <b>{current_topic}</b>", parse_mode="HTML")
    
    except telegram.error.TimedOut:
        await update.message.reply_text("⚠️ Telegram ci sta mettendo troppo a elaborare il tuo PDF. Prova a inviarlo di nuovo!")




# EXECUTION

if __name__ == '__main__':
    TOKEN = os.getenv("TELEGRAM_TOKEN")
    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .connect_timeout(60)
        .read_timeout(60)
        .write_timeout(60)
        .build()
    )

    # Caricamento degli Handler
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("topic", set_topic))
    app.add_handler(CommandHandler("topics", topics))
    app.add_handler(CommandHandler("cancella", delete_topic))


    app.add_handler(MessageHandler(filters.VOICE | filters.AUDIO, handle_speech))
    app.add_handler(MessageHandler(filters.Document.PDF, handle_pdf))

    
    print("BOT in ascolto")

    app.run_polling()



