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

def get_connection(id):
    dir = os.path.dirname(os.path.abspath(__file__))

    # Cria uma pasta para o nó
    node_dir = os.path.join(dir, str(id))
    os.makedirs(node_dir, exist_ok=True)

    bd_commit = os.path.join(node_dir, "commit.db")
    bd_nocommit = os.path.join(node_dir, "nocommit.db")

    commit_connection = sqlite3.connect(bd_commit)
    nocommit_connection = sqlite3.connect(bd_nocommit)

    return commit_connection, nocommit_connection

def create_bd(id):
    commit_connection, nocommit_connection = get_connection(id)

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
            dado TEXT NOT NULL,
            voted_for BOOLEAN
        )
    """)

    nocommit_connection.commit()
    nocommit_connection.close()

def insert_log_entry(id, indice, termo, dado, voted_for):
    _, nocommit_connection = get_connection(id)
    cursor = nocommit_connection.cursor()

    cursor.execute("""
        INSERT INTO log_entries (indice, termo, dado, voted_for)
        VALUES (?, ?, ?, ?)
    """, (indice, termo, dado, voted_for))

    nocommit_connection.commit()
    nocommit_connection.close()

def commit_log_entry(id, indice):
    commit_connection, nocommit_connection = get_connection(id)

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
        cursor_commit.exceutemany("""
            INSERT INTO commited_data (indice, termo, dado)
            VALUES (?, ?, ?)
        """, log_entries)

        # remove entradas já commitadas
        cursor_nocommit.execute("DELETE FROM log_entries WHERE indice <= ?", (indice,))

    commit_connection.commit()
    nocommit_connection.commit()

    commit_connection.close()
    nocommit_connection.close()

# funções auxiliares
# def get_voted_for(id):
    
# def set_voted_for(id):
    
# def get_voted_for(id):