import os
import json
import pypdf
import requests
from groq import Groq
from confluent_kafka import Consumer
from elasticsearch import Elasticsearch
from datetime import datetime


# -- Setup -- #
KAFKA_BROKER = "kafka:9092"
ELASTIC_HOST = "http://elasticsearch:9200"
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
# per inviare il feedback a telegram:
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")




# Configurazione Groq
print("Inizializzazione client Groq...")
client = Groq(api_key=GROQ_API_KEY)

# Configurazione Whisper
print("Inizializzazione Whisper (remoto)...")
whisper_model = "whisper-large-v3"

# Configurazione Elasticsearch
print("Inizializzazione ElasticSearch...")
elase = Elasticsearch(ELASTIC_HOST)

# Configurazione Consumer di Kafka
print("Inizializzazione Consumer per Kafka...")
config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'spark-processor-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(config)
# "Iscrizione" al topic di interesse
# tc-input = topic su cui scrive il producer Kafka (consultare main.py in /bot)
consumer.subscribe(['tc-input'])

print("Spark Consumer configurato correttamente\nIn ascolto sul topic 'tc-input'...")



# -- GESTIONE feedback --> TELEGRAM.bot -- #
def send_feedback(user_id, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": user_id,
        "text": text,
        "parse_mode": "HTML"
    }

    try:
        post = requests.post(url, json=payload)
        print(f"\nCHECK post.telegram : {post.status_code} -- {post.text}\n")
    except Exception as e:
        print(f"Errore invio Telegram: {e}")








# -- FUNZIONI PER MANIPOLAZIONE FILE -- #

def extract_text_from_pdf(path):
    text = ""
    print("\nEstrazione testo dal pdf in corso...\n")
    try:
        with open(path, "rb") as f:
            reader = pypdf.PdfReader(f)
            for page in reader.pages:
                print(f"Lettura pagina {page}...\n")
                extracted = page.extract_text()
                if extracted:
                    text += extracted + " "
    except Exception as e:
        print(f"Errore lettura PDF: {e}")
    
    print("\nLettura PDF conclusa.")
    return text

#----



# -- FUNZIONI PER L'ANALISI DEI DATI -- #

""" Le metriche di nostro interesse sono:
- WPM = words per minute
- PAU = Numero di pause (prese in considerazione solo se maggiori o uguali a 2 secondi)
- SPR (Speech/Pause ratio) = percentuale di tempo parlato effettivo rispetto al totale, tenendo conto delle pause

"""
def speech_metrics(segments):
    if not segments: return 0, 0, 0

    total_duration = segments[-1]['end']
    total_words = sum(len(s['text'].split()) for s in segments)
    speaking_time = sum(s['end'] - s['start'] for s in segments)

    # Calcolo WPM
    wpm = (total_words / (total_duration / 60)) if total_duration > 0 else 0

    # Calcolo PAU (pause)
    pau = 0
    for i in range(len(segments)-1):
        if segments[i+1]['start'] - segments[i]['end'] > 2.0:
            pau += 1

    # Calcolo SPR
    spr = (speaking_time / total_duration) * 100 if total_duration > 0 else 0


    # Ritorna, in ordine: WPM, PAU, SPR
    return round(wpm,1), pau, round(spr,1)



## Per questioni di efficienza e consumo delle risorse, passiamo al modello remoto di Whisper invece di effettuare la trascrizione dell'audio in locale.
## Usiamo sempre il client Groq, il quale offre anche il modello Whisper 3
def transcribe_audio(file_path):
    print(f"Trascrizione tramite modello remoto Whisper per il file: {file_path} in corso...\n")
    with open(file_path, "rb") as file:
        transcription = client.audio.transcriptions.create(
            file=(file_path, file),
            model=whisper_model,
            response_format="verbose_json",
        )
    return transcription





def groq_analysis(audio_text, pdf_text=None):
    if pdf_text:
        sys_prompt = "Sei un tutor che confronta una ripetizione con un testo di riferimento. Non descrivere il contenuto dell'esposizione orale e del testo, dai solo un giudizio"
        user_content = f"TESTO PDF: {pdf_text[:2000]}\nTRASCRIZIONE AUDIO: {audio_text}"
    else:
        sys_prompt = "Sei un tutor che analizza un'esposizione orale libera. Non descrivere il contenuto dell'esposizione orale, dai solo un giudizio."
        user_content = f"TRASCRIZIONE AUDIO: {audio_text}"

    prompt = f"""
    Analizza il contenuto e restituisci un JSON con:
    - "sentiment": (Positivo/Neutro/Negativo)
    - "tono": (es. Sicuro, Ansioso, Esitante, Monotono)
    - "feedback_contenuto": (Analisi concettuale)
    - "consigli": (Come migliorare)
    """

    try:
        completion = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_content + "\n\n" + prompt}
            ],
            response_format={"type": "json_object"}
        )

        return json.loads(completion.choices[0].message.content)
    
    except Exception as e:
        print(f"Errore Groq: {e}")
        return {"sentiment":"N/D", "tono":"N/D", "feedback_contenuto":"Errore", "consigli":"Riprova"}




# -- MAIN -- #

print("\nConsumer di Spark in ascolto...")

try:
    while True:
        pdf_ref = None
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            print(f"Errore Kafka: {msg.error()}")
            continue

        data = json.loads(msg.value().decode('utf-8'))
        user_id = str(data['user_id'])
        topic = data.get('topic', 'generale')
        file_type = data['type']
        file_path = data.get('path')

        pdf_id = f"{user_id}_{topic}"



        # GESTIONE PDF
        if file_type == 'pdf':
            print(f"Indicizzazione PDF per topic: {topic}")
            content = extract_text_from_pdf(file_path)
            elase.index(index="student-pdfs", id=pdf_id, document={
                "user_id": user_id,
                "topic": topic,
                "text": content,
                "timestamp": datetime.now()
            })



        # GESTIONE AUDIO
        elif file_type == 'audio':
            print(f"Analisi Audio per topic: {topic}")

            # Trascrizione Whisper (remoto)
            result = transcribe_audio(file_path)

            # Conversione da lista a dict
            segments = [s if isinstance(s, dict) else (s.model_dump() if hasattr(s, 'model_dump') else vars(s)) for s in result.segments]

            # Calcolo metriche
            wpm, pau, spr = speech_metrics(segments)
            
            # Recupero PDF opzionale
            pdf_ref = None
            try:
                res = elase.get(index="student-pdfs", id=pdf_id)
                pdf_ref = res['_source']['text']
            except:
                print("Modalità analisi libera (nessun PDF trovato)")

            # Analisi Groq
            analysis = groq_analysis(result.text, pdf_ref)

            # Salvataggio finale
            final_doc = {
                "user_id": user_id,
                "topic": topic,
                "timestamp": datetime.now(),
                "metrics": {"wpm": wpm, "pauses": pau, "speech_pause_ratio": spr},
                "analysis": analysis,
                "text": result.text
            }
            elase.index(index="thesis-analysis", document=final_doc)
            print("Analisi salvata su Elasticsearch.")

            # Messaggio di ritorno al Bot
            print(f"Invio del messaggio di feedback a telegram all'user_id: {user_id}")

            # Recupero feedback_contenuto)            
            content =  str(analysis.get('feedback_contenuto', 'N/D'))
            
            # Recupero Tono e Sentiment dall'analisi
            tone = str(analysis.get('tono', 'N/D'))
            sentiment = str(analysis.get('sentiment', 'N/D'))

            # Preparazione alla stampa (consigli)
            tips = analysis.get('consigli', [])

            if isinstance(tips, list) and len(tips) > 0:
                formatted_tips = "\n".join([f"• {tip}" for tip in tips])
            else:
                formatted_tips = "Nessun consiglio particolare"


            feedback = (
                "✅ <b>ANALISI COMPLETATA</b>\n\n"
                f"📚 <b>Topic:</b> {topic}\n"
                f"🚀 <b>Velocità:</b> {wpm} WPM\n"
                f"🌊 <b>Fluidità:</b> {spr}%\n"
                f"⏸️ <b>Pause:</b> {pau}\n"
                f"🤔 <b>Tono:</b> {tone}\n"
                f"🤩 <b>Sentiment:</b> {sentiment}\n\n"
                "📝 <b>Analisi in breve:</b>\n"
                f"{content}\n\n"
                "💡 <b>Consigli:</b>\n"
                f"{formatted_tips}\n\n"
                "📊 <a href='http://localhost:5601'>Apri la Dashboard su Kibana (solo PC)</a>"
            )
            send_feedback(user_id, feedback)
            
            # Rimozione file audio locale
            if os.path.exists(file_path):
                os.remove(file_path)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
