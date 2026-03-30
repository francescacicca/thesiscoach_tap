# 🤖 Thesis Coach
Progetto per l'esame finale di TAP. 
Si tratta di un Bot Telegram che aiuta lo studente a migliorare le proprie performance in vista della discussione della tesi di laurea. L'architettura si basa sull'uso di più microservizi, quali Spark, ElasticSearch, Kafka, Kibana (per monitorare i propri progressi).



## Guida alla configurazione
*Per l'utilizzo, nella cartella del sistema va creata una ulteriore cartella "data" vuota, contenente due sottocartelle vuote, da ridenominare "audio" e "pdf"
*Il sistema utilizza una API Key fornita da Groq.com per l'accesso ad alcuni modelli remoti. E' possibile generarne una gratuitamente sulla pagina per sviluppatori, quindi copiarla e salvarla in una variabile d'ambiente '.env', col nome 'GROQ_API_KEY'. Attenzione: la variabile deve trovarsi nella cartella generale del sistema.
*Il sistema basa la propria interfaccia utente sull'uso di un bot Telegram, dunque è necessario crearne uno tramite @BotFather, copiarne il token e memorizzare anche questo nella variabile d'ambiente '.env', usando il nome 'TELEGRAM_TOKEN'.
