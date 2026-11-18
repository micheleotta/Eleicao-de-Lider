'''
Persistência em Disco: persistir em disco o termo, o voto concedido,
o log de operações e os dados comitados.


Cada nó armazena:
1. estado do nó para resistir a desligamentos
    - termo atual
    - voto concedido (voted_for)

2. dados comitados, estado do banco final (commited_data)
    - índice
    - termo
    - dado
    
3. dados não comitados, log de operações (log_entries)
    - índice
    - termo
    - dado
'''

import sqlite3
import os
import no_pb2

from typing import List


def get_node_dir(id):
    '''Retorna o diretório dos bancos do nó'''
    dir = os.path.dirname(os.path.abspath(__file__))
    node_dir = os.path.join(dir, f"no_{str(id)}")
    return node_dir


def get_connection(id, bd_name):
    '''Retorna a conexão do banco específico'''
    node_dir = get_node_dir(id)
    bd = os.path.join(node_dir, f"{bd_name}.db")
    return sqlite3.connect(bd)


def initialize_bd(id):
    '''Inicializa todos os bancos (estado do nó, dados commitados e dados não commitados)'''
    # Cria uma pasta para o nó
    node_dir = get_node_dir(id)
    os.makedirs(node_dir, exist_ok=True)

    estado_connection = get_connection(id, "estado")
    commit_connection = get_connection(id, "commited_data")
    nocommit_connection = get_connection(id, "log_entries")

    # banco de estado do nó
    cursor = estado_connection.cursor()

    # estado do nó (termo atual e voto concedido)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS estado (
            termo INTEGER PRIMARY KEY,
            voted_for INTEGER
        )
    """)

    estado_connection.commit()
    estado_connection.close()


    # banco de dados commitados
    cursor = commit_connection.cursor()

    # dados efetivamente comitados
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS commited_data (
            indice INTEGER PRIMARY KEY,
            termo INTEGER NOT NULL,
            dado TEXT NOT NULL
        )
    """)

    commit_connection.commit()
    commit_connection.close()


    # banco de dados não commitados
    cursor = nocommit_connection.cursor()

    # log de operações, incluindo entradas ainda não comitadas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS log_entries (
            indice INTEGER PRIMARY KEY,
            termo INTEGER NOT NULL,
            dado TEXT NOT NULL
        )
    """)

    nocommit_connection.commit()
    nocommit_connection.close()


def get_estado(id):
    '''Retorna o termo e votação do nó'''
    connection = get_connection(id, "estado")
    cursor = connection.cursor()

    cursor.execute("""
        SELECT termo, voted_for FROM estado
        ORDER BY termo DESC LIMIT 1
    """)

    data = cursor.fetchone()
    estado = {"termo": data[0], "voted_for": data[1]} if data else None

    connection.close()
    return estado


def update_estado(id, termo, voted_for):
    '''Atualiza o termo e votação do nó'''
    connection = get_connection(id, "estado")
    cursor = connection.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO estado (termo, voted_for)
        VALUES (?, ?)
    """, (termo, voted_for))

    connection.commit()
    connection.close()


def get_log_entries(id, termo, indice) -> List[no_pb2.Log]:
    '''Retorna os logs não commitadosa partir do termo e índice'''
    connection = get_connection(id, "log_entries")
    cursor = connection.cursor()

    cursor.execute("""
        SELECT indice, termo, dado FROM log_entries
        WHERE termo >= ? AND indice >= ?
        ORDER BY indice
    """, (termo, indice))
    rows = cursor.fetchall()

    log_entries = [no_pb2.Log(indice=row[0], termo=row[1], dado=row[2]) for row in rows]

    connection.close()
    return log_entries


def insert_log_entries(id, log_entries: List[no_pb2.Log]):
    '''Insere uma lista de entradas de log'''
    nocommit_connection = get_connection(id, "log_entries")
    cursor = nocommit_connection.cursor()

    cursor.executemany("""
        INSERT INTO log_entries (indice, termo, dado)
        VALUES (?, ?, ?)
    """, [(entry.indice, entry.termo, entry.dado) for entry in log_entries])

    nocommit_connection.commit()
    nocommit_connection.close()


def commit_logs(id, indice):
    '''Faz o commit dos logs (do último commit até o índice informado)'''
    commit_connection = get_connection(id, "commited_data")
    nocommit_connection = get_connection(id, "log_entries")

    cursor_nocommit = nocommit_connection.cursor()
    cursor_commit = commit_connection.cursor()

    # busca o último índice commitado
    cursor_commit.execute("SELECT MAX(indice) FROM commited_data")
    commit_index = cursor_commit.fetchone()[0]

    if (commit_index is not None) and (indice <= commit_index):
        print(f"Índice {indice} já comitado. Ignorando commit.")
        nocommit_connection.close()
        commit_connection.close()
        return False

    # busca os logs entre o último commit e o índice informado
    cursor_nocommit.execute("""
        SELECT indice, termo, dado FROM log_entries
        WHERE indice > ? AND indice <= ?
    """, (commit_index, indice))
    log_entries = cursor_nocommit.fetchall()

    if len(log_entries) > 0:
        cursor_commit.executemany("""
            INSERT INTO commited_data (indice, termo, dado)
            VALUES (?, ?, ?)
        """, log_entries)

    commit_connection.commit()

    commit_connection.close()
    nocommit_connection.close()
    return True


def get_last_commit() -> no_pb2.Log:
    '''Retorna o último commit'''
    connection = get_connection(id, "commited_data")
    cursor = connection.cursor()

    cursor.execute("""
        SELECT indice, termo, dado FROM commited_data
        ORDER BY indice DESC LIMIT 1
    """)
    data = cursor.fetchone()

    last_commit = no_pb2.Log(indice=data[0], termo=data[1], dado=data[2]) if data else None

    connection.close()
    return last_commit
