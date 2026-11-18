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

import sqlite3
import os

from typing import List

class LogEntry:
    def __init__(self, indice, termo, dado):
        self.indice = indice
        self.termo = termo
        self.dado = dado

def get_node_dir(id):
    dir = os.path.dirname(os.path.abspath(__file__))
    node_dir = os.path.join(dir, str(id))
    return node_dir

def get_connection(id, bd_name):
    node_dir = get_node_dir(id)
    bd = os.path.join(node_dir, f"{bd_name}.db")
    return sqlite3.connect(bd)

def initialize_bd(id):
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
            voted_for INTEGER NOT NULL
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

def update_estado(id, termo, voted_for):
    connection = get_connection(id, "estado")
    cursor = connection.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO estado (termo, voted_for)
        VALUES (?, ?)
    """, (termo, voted_for))

    connection.commit()
    connection.close()

def insert_log_entries(id, log_entries: List[LogEntry]):
    nocommit_connection = get_connection(id, "log_entries")
    cursor = nocommit_connection.cursor()

    cursor.executemany("""
        INSERT INTO log_entries (indice, termo, dado)
        VALUES (?, ?, ?)
    """, [(entry.indice, entry.termo, entry.dado) for entry in log_entries])

    nocommit_connection.commit()
    nocommit_connection.close()

def commit_logs(id, indice):
    commit_connection = get_connection(id, "commited_data")
    nocommit_connection = get_connection(id, "log_entries")

    cursor_nocommit = nocommit_connection.cursor()
    cursor_commit = commit_connection.cursor()

    # busca o índice no banco de dados commitado
    cursor_commit.execute("SELECT MAX(indice) FROM commited_data")
    commit_index = cursor_commit.fetchone()[0]

    if (commit_index is not None) and (indice <= commit_index):
        print(f"Índice {indice} já comitado. Ignorando commit.")
        nocommit_connection.close()
        commit_connection.close()
        return

    cursor_nocommit.execute("""
        SELECT indice, termo, dado FROM log_entries
        WHERE indice <= ?
    """, (indice,))
    log_entries = cursor_nocommit.fetchall()

    if len(log_entries) > 0:
        cursor_commit.executemany("""
            INSERT INTO commited_data (indice, termo, dado)
            VALUES (?, ?, ?)
        """, log_entries)

    commit_connection.commit()
    nocommit_connection.commit()

    commit_connection.close()
    nocommit_connection.close()

# funções auxiliares
# def get_voted_for(id):
    
# def set_voted_for(id):
    
# def get_voted_for(id):