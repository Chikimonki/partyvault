import redis
import time

r = redis.Redis(host='localhost', port=6379, db=0)

def main():
    # Create consumer group if not exists
    try:
        r.xgroup_create('parties', 'risk_alerts', id='0', mkstream=True)
    except redis.exceptions.ResponseError as e:
        if 'BUSYGROUP' not in str(e):
            raise

    print("Listening for high-risk parties...")
    while True:
        # Read from stream with consumer group
        results = r.xreadgroup('risk_alerts', 'consumer1', {'parties': '>'}, count=1, block=1000)
        for stream, messages in results:
            for msg_id, fields in messages:
                # Decode bytes to strings
                fields = {k.decode(): v.decode() for k, v in fields.items()}
                risk_score = int(fields.get('risk_score', 0))
                if risk_score >= 70:
                    print(f"🚨 ALERT: High-risk party {fields['id']} ({fields['name']}) risk score {risk_score}")
                else:
                    print(f"ℹ️ Party {fields['id']} risk score {risk_score} (ok)")
                # Acknowledge the message
                r.xack('parties', 'risk_alerts', msg_id)

if __name__ == '__main__':
    main()
