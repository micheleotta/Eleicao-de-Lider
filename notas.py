
'''
cada nó armazena:
NODE
TERM                -> representa o mandato da liderança atual
ÍNDICE DE LOG       -> SET
ÍNDICE DE COMMIT    -> o maior índice do log que já foi replicado pelo quórum
LEADER
'''

# ao ser iniciado, o nó = estado.seguidor

# se não escutam de um líder -> estado.candidato

# ELEIÇÃO DE LÍDER
# requisita votos de outros nós
# respondem com os votos -> voto único por termo (se log candidato >= log próprio)
# se maioria dos votos eleito -> estado.lider

# quorum fixo para cluster 4 nós -> 3 nós


# se SEGUIDOR
# randomized to be between 150ms and 300ms

# append entries (heartbeat timeout) Fetch Request -> avisar que tá vivo ao LÍDER
# enviar termo e último índice
# ex: a cada 100 ms
# recebe resposta -> reinicia temporizador
'''
Loop de Fetch Ativo: busca ativamente entradas no líder atual e
aplica o índice de commit recebido para efetivar os dados no banco local,
tornando-os visíveis para leitura.
'''
# *******ao detectar divergências de log, o nó apaga o log local a partir do ponto conflitante e ressincroniza com o líder atual.

# se consulta de cliente -> retorna com dados comitados

# ELECTION TIMEOUT
# term += 1
# começa new election term -> envia request vote

# se LIDER
# temporizador heartbeat timeout para cada réplica -> se parar, considera-a inativa
# se último fetch request ultrapassar heartbeat timeout: replica = inativa
# se ativas >= quorum: permite escritas do cliente, se não: recusa e informa

# ESCRITA POR CLIENTE   ==== LOG REPLICATION append entries
# Each change is added as an entry in the node's log -> uncommited
# registra novas entradas com seu número de termo atual e índice do log (aumenta o log)
# replica entrada aos seguidores
# espera maioria dos nós escreverem a entrada (quorum fixo) -> commita e indice commit += 1
# novo Fetch Request dos bichinhos ao lider -> grava no banco local
# nofica seguidores entrada commitada
'''
Índice de commit: o maior índice do log que já foi replicado e
armazenado na maioria dos nós (quórum). Dados com índice do log
≤ índice do commit são considerados comitados/efetivados e aplicados
no banco de dados final.
'''

'''
Na execução da aplicação devem ser exibidos: o papel do
nó (seguidor/candidato/líder), termo atual, índice de commit.
'''