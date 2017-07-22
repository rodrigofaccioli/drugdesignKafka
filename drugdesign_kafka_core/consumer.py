from kafka import KafkaConsumer


def consumeMsgFromTopic(topic_name, brokers, group_id='default_group', auto_offset_reset='earliest', timeout_ms=10):

    consumer = KafkaConsumer(topic_name,
                             bootstrap_servers=brokers,
                             group_id=group_id,
                             auto_offset_reset= auto_offset_reset,
                             consumer_timeout_ms=timeout_ms)

    message_list = []
    print ('Start consuming')
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))
        message_list.append(message)

    return message_list