"""
Cliente NFS-like en Python.

Modos de prueba (--test):
  session   → Punto 7: demuestra semántica de sesión (stateless vs stateful)
  dual      → Punto 7: dos clientes en paralelo modificando el mismo archivo
  shutdown  → Punto 9: qué ocurre cuando el servidor se apaga durante el acceso
"""

import socket
import json
import time
import sys
import threading
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

def test_session_semantics(host: str, port: int, client_id: str):
    """
    NFS tradicional (v2/v3) es STATELESS:
      - Cada RPC es independiente; el servidor no recuerda al cliente entre llamadas.
      - No existe concepto de "abrir archivo": cada READ/WRITE lleva offset + tamaño.
      - Si el servidor se reinicia, el cliente reintenta sin saber que hubo caída.

    NFS v4 introduce state (locks, open/close), pero la semántica básica de datos
    sigue siendo por operación, no por sesión de proceso.

    Esta demo muestra que dos conexiones distintas al mismo archivo son independientes:
    el servidor no distingue "sesiones".
    """
    sock = make_connection(host, port)
    if not sock:
        print(f"[Cliente {client_id}] No se pudo conectar.")
        return

    print(f"\n[Cliente {client_id}] === Punto 7: Semántica de sesión ===")
    print("[Cliente {0}] NFS es STATELESS: el servidor no recuerda entre llamadas.".format(client_id))

    archivo = f"sesion_{client_id}.txt"

    # Operación 1: WRITE (sin "abrir" el archivo)
    r = send(sock, {"op": "WRITE", "file": archivo, "content": f"Escrito por cliente {client_id}\n"})
    print(f"[Cliente {client_id}] WRITE:  {r['status']}")

    # Operación 2: APPEND (el servidor no sabe que es el mismo proceso)
    r = send(sock, {"op": "APPEND", "file": archivo, "content": f"Append por cliente {client_id}\n"})
    print(f"[Cliente {client_id}] APPEND: {r['status']}")

    # Operación 3: READ
    r = send(sock, {"op": "READ", "file": archivo})
    print(f"[Cliente {client_id}] READ:\n{r.get('content', r)}")

    sock.close()


def test_dual_clients(host: str, port: int):
    """
    Dos clientes acceden al mismo archivo simultáneamente.
    Sin locking distribuido (NFS v3), las escrituras se intercalan: last-write-wins.
    """
    print("\n=== Punto 7: Dos clientes en paralelo (mismo archivo) ===")
    print("Sin locking: las escrituras se intercalan — last-write-wins.\n")

    shared_file = "compartido.txt"

    def worker(client_id: str, delay: float):
        sock = make_connection(host, port)
        if not sock:
            return
        time.sleep(delay)
        for i in range(3):
            r = send(sock, {"op": "APPEND", "file": shared_file,
                            "content": f"[Cliente {client_id}] línea {i}\n"})
            print(f"[Cliente {client_id}] APPEND línea {i}: {r['status']}")
            time.sleep(0.1)
        sock.close()

    # Limpiar archivo
    s = make_connection(host, port)
    send(s, {"op": "WRITE", "file": shared_file, "content": ""})
    s.close()

    t1 = threading.Thread(target=worker, args=("A", 0.0))
    t2 = threading.Thread(target=worker, args=("B", 0.05))
    t1.start(); t2.start()
    t1.join();  t2.join()

    # Leer resultado final
    sock = make_connection(host, port)
    r = send(sock, {"op": "READ", "file": shared_file})
    sock.close()
    print(f"\n[Resultado] Contenido final de '{shared_file}':\n{r.get('content', r)}")


# ── Punto 9: Servidor apagado durante acceso ──────────────────────────────────

def test_server_shutdown(host: str, port: int):
    """
    Equivalente a un cliente con soft mount:
      - Hard mount (real NFS): el cliente se bloquea indefinidamente hasta que
        el servidor vuelve (usado para datos críticos).
      - Soft mount (real NFS): el cliente recibe ETIMEDOUT y puede continuar.

    Esta demo simula soft mount: detecta el error y lo reporta.
    """
    print("\n=== Punto 9: Servidor apagado durante acceso ===")
    sock = make_connection(host, port)
    if not sock:
        return

    send(sock, {"op": "WRITE", "file": "punto9.txt", "content": "Inicio\n"})
    print("[Cliente] Archivo creado. Accediendo continuamente...")
    print("[Cliente] >>> Apagá el servidor (Ctrl+C en la otra terminal) <<<\n")

    for i in range(60):
        try:
            r = send(sock, {"op": "APPEND", "file": "punto9.txt",
                            "content": f"Escritura {i} a las {time.strftime('%H:%M:%S')}\n"})
            print(f"[Cliente] Escritura {i:02d}: OK")
            time.sleep(0.5)

        except (ConnectionResetError, BrokenPipeError, OSError) as e:
            print(f"\n[Cliente] !!! SERVIDOR CAÍDO !!! — Error: {e}")
            print("[Cliente] Soft mount: el cliente recibe error y puede decidir reintentar.")
            print("[Cliente] Hard mount: el cliente quedaría bloqueado esperando al servidor.")

            # Reintento de reconexión (equivale a que el servidor vuelva)
            print("\n[Cliente] Intentando reconectar...")
            sock = make_connection(host, port, retries=5)
            if sock:
                print("[Cliente] Reconectado al servidor.")
            else:
                print("[Cliente] Servidor aún no disponible. Fin.")
            break

    if sock:
        sock.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Cliente NFS-like")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--client-id", default="1", metavar="ID")
    parser.add_argument("--test", choices=["session", "dual", "shutdown", "all"],
                        default="all", help="Prueba a ejecutar")
    args = parser.parse_args()

    if args.test in ("session", "all"):
        test_session_semantics(args.host, args.port, args.client_id)

    if args.test in ("dual", "all"):
        test_dual_clients(args.host, args.port)

    if args.test == "shutdown":
        test_server_shutdown(args.host, args.port)

    if args.test == "all":
        print("\n=== Para el Punto 9 ejecutá: python client.py --test shutdown ===")


if __name__ == "__main__":
    main()
