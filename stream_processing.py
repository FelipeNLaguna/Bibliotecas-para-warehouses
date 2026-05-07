from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types

def check_fraud_velocity(transaction_stream):
    # Agrupa o fluxo de eventos de forma paralela usando o ID do usuário (key_by)
    # Abre uma janela de tempo de 5 minutos para cada usuário
    alerts = transaction_stream \
       .key_by(lambda txn: txn['user_id']) \
       .window(TumblingEventTimeWindows.of(Time.minutes(5))) \
       .reduce(
            # Função que soma os valores das transações dentro da janela
            lambda txn1, txn2: {
                'user_id': txn1['user_id'], 
                'total_amount': txn1['total_amount'] + txn2['total_amount']
            }
        ) \
       .filter(lambda result: result['total_amount'] > 10000) # Limite de fraude
        
    return alerts
