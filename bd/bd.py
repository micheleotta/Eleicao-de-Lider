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


def get_log_entry(id, indice):
    '''Retorna entrada específica do log'''
    connection = get_connection(id, "log_entries")
    cursor = connection.cursor()

    cursor.execute("""
        SELECT indice, termo, dado FROM log_entries
        WHERE indice = ?
    """, (indice,))
    
    data = cursor.fetchone()
    connection.close()
    
    if data is None:
        return None

    log_entry = no_pb2.Log(indice=data[0], termo=data[1], dado=data[2])
    return log_entry

def get_log_entries(id, indice) -> List[no_pb2.Log]:
    '''Retorna os logs não comitados a partir do índice informado'''
    connection = get_connection(id, "log_entries")
    cursor = connection.cursor()

    cursor.execute("""
        SELECT indice, termo, dado FROM log_entries
        WHERE indice >= ?
        ORDER BY indice
    """, (indice,))
    rows = cursor.fetchall()

    log_entries = [no_pb2.Log(indice=row[0], termo=row[1], dado=row[2]) for row in rows]

    connection.close()
    return log_entries


def insert_log_entries(id, log_entries: List[no_pb2.Log]):
    '''Insere ou atualiza lista de entradas do log'''
    nocommit_connection = get_connection(id, "log_entries")
    cursor = nocommit_connection.cursor()

    cursor.executemany("""
        INSERT INTO log_entries (indice, termo, dado)
        VALUES (?, ?, ?)
        ON CONFLICT(indice) DO UPDATE SET
            termo = excluded.termo,
            dado = excluded.dado
    """, [(entry.indice, entry.termo, entry.dado) for entry in log_entries])

    nocommit_connection.commit()
    nocommit_connection.close()


def truncate_logs_from(id, indice):
    '''Remove entradas conflitantes e todas as posteriores'''

    connection = get_connection(id, "log_entries")
    cursor = connection.cursor()

    cursor.execute("""
        DELETE FROM log_entries
        WHERE indice >= ?
    """, (indice,))

    connection.commit()
    connection.close()


def commit_logs(id, indice):
    '''Faz o commit dos logs (do último commit até o índice informado)'''
    commit_connection = get_connection(id, "commited_data")
    log_connection = get_connection(id, "log_entries")

    try:
        cursor_log = log_connection.cursor()
        cursor_commit = commit_connection.cursor()

        # busca o último índice commitado
        cursor_commit.execute("SELECT MAX(indice) FROM commited_data")
        commit_index = cursor_commit.fetchone()[0]
        
        if commit_index is None:
            commit_index = 0

        if indice <= commit_index:
            return False

        # busca os logs entre o último commit e o índice informado
        cursor_log.execute("""
            SELECT indice, termo, dado FROM log_entries
            WHERE indice > ? AND indice <= ?
            ORDER BY indice
        """, (commit_index, indice))
        log_entries = cursor_log.fetchall()

        # Não permite avançar o commit se houver lacunas
        if len(log_entries) != indice - commit_index:
            raise Exception(f"Log incompleto: esperado até índice {indice}")
        
        # Aplica as entradas no banco final
        cursor_commit.executemany("""
            INSERT INTO commited_data (indice, termo, dado)
            VALUES (?, ?, ?)
        """, log_entries)

        commit_connection.commit()
        return True
    
    except Exception:
        commit_connection.rollback()
        raise

    finally:
        commit_connection.close()
        log_connection.close()


def get_last_commit(id) -> no_pb2.Log:
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


def get_commit(id, indice) -> no_pb2.Log:
    '''Retorna o commit pelo index'''
    connection = get_connection(id, "commited_data")
    cursor = connection.cursor()

    cursor.execute("""
        SELECT indice, termo, dado FROM commited_data
        WHERE indice = ?
    """, (indice,))
    data = cursor.fetchone()

    commit = no_pb2.Log(indice=data[0], termo=data[1], dado=data[2]) if data else None

    connection.close()
    return commit


def get_last_log_index(id):
    '''Retorna o maior índice existente no log'''

    connection = get_connection(id, "log_entries")
    cursor = connection.cursor()

    cursor.execute("""
        SELECT MAX(indice)
        FROM log_entries
    """)

    indice = cursor.fetchone()[0]
    connection.close()

    return indice if indice is not None else 0


def get_last_commit_index(id):
    '''Retorna o maior índice comitado'''

    connection = get_connection(id, "commited_data")
    cursor = connection.cursor()

    cursor.execute("""
        SELECT MAX(indice)
        FROM commited_data
    """)

    indice = cursor.fetchone()[0]
    connection.close()

    return indice if indice is not None else 0