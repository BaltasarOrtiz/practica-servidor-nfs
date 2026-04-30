"""
Cliente NFS-like en Python.

Modos de prueba (--test):
  session   → Punto 7: demuestra semántica de sesión (stateless)
  shutdown  → Punto 9: qué ocurre cuando el servidor se apaga durante el acceso
"""

import socket
import json
import time
import sys
import argparse


# ── Primitivas de comunicación ────────────────────────────────────────────────

def make_connection(host: str, port: int, retries: int = 3) -> socket.socket | None:
    for attempt in range(1, retries + 1):
        try:
            sock = socket.create_connection((host, port), timeout=5)
            return sock
        except ConnectionRefusedError:
            print(f"[-] Intento {attempt}/{retries}: servidor no disponible")
            time.sleep(1)
    return None


def send(sock: socket.socket, payload: dict) -> dict:
    sock.send(json.dumps(payload).encode())
    data = sock.recv(8192)
    return json.loads(data.decode())


# ── Punto 7: Semántica de sesión ──────────────────────────────────────────────

def test_session_semantics(host: str, port: int):
    """
    NFS tradicional (v2/v3) es STATELESS:
      - Cada RPC es independiente; el servidor no recuerda al cliente entre llamadas.
      - No existe concepto de "abrir archivo": cada READ/WRITE lleva offset + tamaño.
      - Si el servidor se reinicia, el cliente reintenta sin saber que hubo caída.

    NFS v4 introduce state (locks, open/close), pero la semántica básica de datos
    sigue siendo por operación, no por sesión de proceso.
    """
    sock = make_connection(host, port)
    if not sock:
        print("[-] No se pudo conectar.")
        return

    print("\n=== Punto 7: Semántica de sesión ===")
    print("NFS es STATELESS: el servidor no recuerda el estado entre llamadas.\n")

    # Operación 1: WRITE (sin "abrir" el archivo, como en NFS real)
    r = send(sock, {"op": "WRITE", "file": "sesion.txt", "content": "Primera escritura\n"})
    print(f"WRITE:  {r['status']}")

    # Operación 2: APPEND — el servidor no sabe que es el mismo proceso
    r = send(sock, {"op": "APPEND", "file": "sesion.txt", "content": "Segunda escritura (append)\n"})
    print(f"APPEND: {r['status']}")

    # Operación 3: READ
    r = send(sock, {"op": "READ", "file": "sesion.txt"})
    print(f"READ:\n{r.get('content', r)}")

    # Operación 4: LIST — también stateless, snapshot del directorio en ese instante
    r = send(sock, {"op": "LIST"})
    print(f"LIST:   {r.get('files', r)}")

    print("\nConclusión: cada operación fue independiente. El servidor no mantuvo")
    print("ningún estado de 'sesión'. Si se reiniciara entre llamadas, el cliente")
    print("podría reintentar sin saber que hubo caída (comportamiento NFS v2/v3).")

    sock.close()


# ── Punto 9: Servidor apagado durante acceso ──────────────────────────────────

def test_server_shutdown(host: str, port: int):
    """
    Equivalente a un cliente con soft mount:
      - Hard mount (NFS real): el cliente se bloquea indefinidamente hasta que
        el servidor vuelve. Se usa para datos críticos.
      - Soft mount (NFS real): el cliente recibe ETIMEDOUT y puede continuar.

    Esta demo simula soft mount: detecta el error, lo reporta e intenta reconectar.
    """
    print("\n=== Punto 9: Servidor apagado durante acceso ===")
    sock = make_connection(host, port)
    if not sock:
        return

    send(sock, {"op": "WRITE", "file": "punto9.txt", "content": "Inicio\n"})
    print("Archivo creado. Accediendo continuamente cada 0.5s...")
    print(">>> Apagá el servidor (Ctrl+C en la otra terminal) para ver el efecto <<<\n")

    for i in range(60):
        try:
            r = send(sock, {"op": "APPEND", "file": "punto9.txt",
                            "content": f"Escritura {i} a las {time.strftime('%H:%M:%S')}\n"})
            print(f"Escritura {i:02d}: OK")
            time.sleep(0.5)

        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            print(f"\n!!! SERVIDOR CAÍDO !!! — {e}")
            print("Soft mount: el cliente recibe error y puede decidir reintentar.")
            print("Hard mount: el cliente quedaría bloqueado esperando al servidor.\n")

            print("Intentando reconectar...")
            sock = make_connection(host, port, retries=5)
            if sock:
                print("Reconectado. Las escrituras perdidas durante la caída NO se recuperan.")
            else:
                print("Servidor aún no disponible. Fin.")
            break

    if sock:
        sock.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Cliente NFS-like")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--test", choices=["session", "shutdown", "all"],
                        default="all", help="Prueba a ejecutar")
    args = parser.parse_args()

    if args.test in ("session", "all"):
        test_session_semantics(args.host, args.port)

    if args.test in ("shutdown",):
        test_server_shutdown(args.host, args.port)

    if args.test == "all":
        print("\n=== Para el Punto 9 ejecutá: python client.py --test shutdown ===")


if __name__ == "__main__":
    main()
