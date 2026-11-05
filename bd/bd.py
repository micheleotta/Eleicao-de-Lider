# Persistir em disco o termo, o voto concedido (flag se já votei), o log de operações e os dados comitados
'''
cada nó armazena:
NODE
TERM                -> representa o mandato da liderança atual
ÍNDICE DE LOG       -> SET
ÍNDICE DE COMMIT    -> o maior índice do log que já foi replicado pelo quórum
LEADER


1. O Termo atual e o Voto concedido (voted_for);
2. O log de operações (entradas com termo e índice);
3. O estado do banco final (dados comitados).
'''