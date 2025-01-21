import pika
from models import Contact

# Підключення до RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

# Створення черги
channel.queue_declare(queue="email_queue")

# Імітація надсилання email
def send_email(contact):
    print(f"Надсилаємо email до {contact.fullname} ({contact.email})...")
    contact.is_sent = True
    contact.save()  # Оновлюємо статус у базі даних
    print(f"Email надіслано контакту {contact.fullname}.")

# Функція обробки повідомлення
def callback(ch, method, properties, body):
    contact_id = body.decode("utf-8")
    contact = Contact.objects(id=contact_id).first()
    if contact:
        send_email(contact)
    else:
        print(f"Контакт із ID {contact_id} не знайдено.")

# Встановлення функції обробника
channel.basic_consume(
    queue="email_queue", on_message_callback=callback, auto_ack=True
)

print("Очікуємо повідомлень...")
channel.start_consuming()