config = {
    "google_api_key": "AIzaSyAJOonfp5ExMDAve5Hi9tyZ-omQ2avWGYw",
    "youtube_playlist_id": "PLnLprvuLGNqLHRP1OVcaKVgOfZPkFMwFE",
    "kafka": {
        # Get this from confluent website.
        "bootstrap.servers": "pkc-l6wr6.europe-west2.gcp.confluent.cloud:9092",
        "security.protocol": "sasl_ssl",
        "sasl.mechanism": "PLAIN",
        # below is API key
        "sasl.username": "NKIHYXLE3KF4QNBG",
        # below is secret
        "sasl.password": "YtABv5gXSMloOsqZ48+IwekIUohpsBd3auMeqPIXaDFiXOVaXx93YRGaIs+qoL/F",
    },
    "schema_registry": {
        "url": "https://psrc-kk5gg.europe-west3.gcp.confluent.cloud",
        "basic.auth.user.info": "63GDVELV4IIKIQQ4:H2PECDUyHZNt+tIiOTmqk+FagCR1lBZ0cuz+lsISxbTQFpEdnBTdzU1LNyfLqAWu",
    },
}
