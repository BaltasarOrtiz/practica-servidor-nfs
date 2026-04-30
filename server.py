"""
Servidor NFS-like en Python.

Puntos cubiertos:
  7 - Semántica de sesión: NFS es STATELESS, cada operación es atómica e independiente.
  8 - ¿El servidor puede ser cliente? SÍ. Usar --as-client HOST:PORT.
  9 - Servidor apagado: el cliente recibe error de conexión (equivalente a soft mount).
"""

import socket
import threading
import json
import os
import signal
import sys
import time
import argparse

HOST = "0.0.0.0"
DEFAULT_PORT = 8000
EXPORT_DIR = "./nfs_export"


# ── Request handler ──────────────────────────────────────────────────────────

def process_request(request: dict) -> dict:
    op       = request.get("op", "")
    filename = request.get("file", "")
    content  = request.get("content", "")
    filepath = os.path.join(EXPORT_DIR, os.path.basename(filename))  # prevent path traversal

    try:
        if op == "READ":
            with open(filepath) as f:
                return {"status": "ok", "content": f.read()}

        elif op == "WRITE":
            with open(filepath, "w") as f:
                f.write(content)
            return {"status": "ok"}

        elif op == "APPEND":
            with open(filepath, "a") as f:
                f.write(content)
            return {"status": "ok"}

        elif op == "LIST":
            return {"status": "ok", "files": os.listdir(EXPORT_DIR)}

        elif op == "DELETE":
            os.remove(filepath)
            return {"status": "ok"}

        else:
            return {"status": "error", "message": f"Operación desconocida: {op}"}

    except FileNotFoundError:
        return {"status": "error", "message": f"Archivo no encontrado: {filename}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def handle_client(conn: socket.socket, addr):
    print(f"[+] Cliente conectado: {addr}")
    try:
        while True:
            data = conn.recv(8192)
            if not data:
                break
            request  = json.loads(data.decode())
            response = process_request(request)
            conn.send(json.dumps(response).encode())
    except (ConnectionResetError, BrokenPipeError):
        pass
    except json.JSONDecodeError as e:
        print(f"[!] JSON inválido de {addr}: {e}")
    finally:
        conn.close()
        print(f"[-] Cliente desconectado: {addr}")


# ── Punto 8: el servidor actuando como cliente ────────────────────────────────

def connect_as_client(target_host: str, target_port: int):
    """
    Punto 8 — El servidor NFS puede ser cliente de otro servidor NFS.
    En el mundo real: un nodo puede exportar /data y montar /backup de otro servidor.
    """
    time.sleep(2)  # esperar a que el servidor destino esté listo
    print(f"\n[Servidor→Cliente] Conectando a {target_host}:{target_port}")
    try:
        sock = socket.create_connection((target_host, target_port), timeout=5)

        def req(payload):
            sock.send(json.dumps(payload).encode())
            return json.loads(sock.recv(8192).decode())

        # Escribir desde el servidor actuando como cliente
        r = req({"op": "WRITE", "file": "desde_servidor.txt",
                 "content": "Este archivo lo creó el servidor actuando como cliente.\n"})
        print(f"[Servidor→Cliente] WRITE: {r}")

        # Leer de vuelta
        r = req({"op": "READ", "file": "desde_servidor.txt"})
        print(f"[Servidor→Cliente] READ:  {r['content'].strip()!r}")

        sock.close()
        print("[Servidor→Cliente] ✓ El servidor SÍ puede ser cliente de otro servidor NFS.")
    except ConnectionRefusedError:
        print("[Servidor→Cliente] No se pudo conectar al servidor destino.")
    except Exception as e:
        print(f"[Servidor→Cliente] Error: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Servidor NFS-like")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Puerto de escucha")
    parser.add_argument("--as-client", metavar="HOST:PORT",
                        help="Punto 8: también conectarse como cliente a HOST:PORT")
    args = parser.parse_args()

    os.makedirs(EXPORT_DIR, exist_ok=True)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, args.port))
    server.listen(10)

    print(f"[NFS Server] Escuchando en {HOST}:{args.port}")
    print(f"[NFS Server] Directorio exportado: {os.path.abspath(EXPORT_DIR)}")
    print("[NFS Server] Ctrl+C para simular apagado (Punto 9)\n")

    # Punto 9: manejo de SIGINT para simular apagado abrupto
    def shutdown(sig, frame):
        print("\n[NFS Server] !!! SERVIDOR APAGADO !!!")
        print("[NFS Server] Los clientes conectados recibirán error de conexión.")
        server.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)

    # Punto 8: lanzar hilo que actúa como cliente
    if args.as_client:
        host, port = args.as_client.rsplit(":", 1)
        t = threading.Thread(target=connect_as_client, args=(host, int(port)), daemon=True)
        t.start()

    while True:
        try:
            conn, addr = server.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
        except OSError:
            break


if __name__ == "__main__":
    main()
