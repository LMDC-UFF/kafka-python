import os
import json
from confluent_kafka import Producer, Consumer, KafkaError


# Biblioteca para Python & Kafka

class kafkaAPI():

    @staticmethod
    def createConsumerWithKerberos(kafka_ip: str, group_id: str, securityProtocol: str, saslMechanism: str,
                                   saslKerberosServiceName: str, kerberosKeyTabPath: str, kerberosUsername: str):

        conf = {
            'bootstrap.servers': kafka_ip,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'security.protocol': securityProtocol,
            'sasl.mechanism': saslMechanism,
            'sasl.kerberos.service.name': saslKerberosServiceName,
            'sasl.kerberos.keytab': kerberosKeyTabPath,
            'sasl.kerberos.principal': kerberosUsername
        }


        c = Consumer(conf)
        return c

    @staticmethod
    def createConsumerWithLoginAndPass(kafka_ip: str, group_id: str, securityProtocol: str, saslMechanism: str,
                                       kafkaUserName: str, kafkaPassword: str):

        conf = {'bootstrap.servers': kafka_ip,
                'group.id': group_id,
                'auto.offset.reset': 'earliest',
                'security.protocol': securityProtocol,
                'sasl.mechanism': saslMechanism,
                'sasl.username':kafkaUserName,
                'sasl.password': kafkaPassword
        }

        c = Consumer(conf)
        return c

    @staticmethod
    def createConsumerWithoutLogin(kafka_ip: str, group_id: str):
        conf = {
            'bootstrap.servers': kafka_ip,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        }

        c = Consumer(conf)
        return c

    @staticmethod
    def send_to_kafka_topic_kerberos(detections: dict, kafka_ip: str, topic_name: str,
                                     kafka_sasl_mechanism: str, kafka_krb5_user_keytab_path: str, kafka_krb5_username: str, kafka_debug: str):
        sasl_mechanism = kafka_sasl_mechanism
        producer_conf = {'bootstrap.servers': kafka_ip,
                         'security.protocol': 'SASL_PLAINTEXT',
                         'sasl.mechanism': sasl_mechanism,
                         'sasl.kerberos.service.name': 'kafka',
                         'sasl.kerberos.keytab': kafka_krb5_user_keytab_path,
                         'sasl.kerberos.principal': kafka_krb5_username}


        try:
            # 'KAFKA_DEBUG' has to be in os.environ
            if kafka_debug.lower() == 'true':
                producer_conf['debug'] = 'security,broker'

            producer = Producer(**producer_conf)

            json_str = json.dumps(detections)
            # Produce line (without newline)
            print('Content sent to kafka! ')  # , json_str.encode('utf8'))
            producer.produce(topic_name, json_str.encode('utf8'))
            producer.poll(0)
            producer.flush()
        except BufferError:
            sys.stderr.write(
                '%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(producer))
            # producer.poll(0)
            # Wait until all messages have been delivered
            # sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
            # p.flush()

    @staticmethod
    def send_to_kafka_topic_without_login(kafka_ip: str, topic_name: str, kafka_sasl_mechanism: str, kafka_debug: str):
        sasl_mechanism = kafka_sasl_mechanism
        producer_conf = {'bootstrap.servers': kafka_ip}

        try:
            # 'KAFKA_DEBUG' has to be in os.environ
            if kafka_debug.lower() == 'true':
                producer_conf['debug'] = 'security,broker'

            producer = Producer(**producer_conf)

            json_str = json.dumps(detections)
            # Produce line (without newline)
            print('Content sent to kafka! ')  # , json_str.encode('utf8'))
            producer.produce(topic_name, json_str.encode('utf8'))
            producer.poll(0)
            producer.flush()
        except BufferError:
            sys.stderr.write(
                '%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(producer))
            # producer.poll(0)
            # Wait until all messages have been delivered
            # sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
            # p.flush()