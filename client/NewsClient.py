
import queue
import socket
import threading
import time


class NewsClient:

    def __init__(self, host="127.0.0.1", port=5000):
        self.host = host
        self.port = port
        self.sock = None
        self.running = False
        self._responses = queue.Queue()
        self._send_lock = threading.Lock()
        self._request_id = 0
        self.news_handler = None
        self.info_handler = None
        self.closed_handler = None

    # handlers
    def set_news_handler(self, fn):

        self.news_handler = fn

    def set_info_handler(self, fn):

        self.info_handler = fn

    def set_closed_handler(self, fn):

        self.closed_handler = fn


    def start(self):
        """inicia a conexão e inicia a thread """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.connect((self.host, self.port))
        self.running = True
        threading.Thread(target=self._receive_loop, daemon=True).start()
        self.send_command("CONECTAR")
        self._iniciar_ping()

    def _iniciar_ping(self):
        pass

    def stop(self):
        """encerra a conexão com o servidor """
        self.running = False
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass

    def _receive_loop(self):
        """le continuamente os datagramas enviados pelo servidor"""
        try:
            while self.running:
                try:
                    data = self.sock.recv(8192)
                except ConnectionResetError:
                    if self.running:
                        continue
                    break
                except OSError:
                    break
                self._dispatch(data.decode("utf-8"))
        finally:
            self.running = False
            if self.closed_handler:
                self.closed_handler()

    def _dispatch(self, line):
        """classifica as mensagens do servidor"""
        if line.startswith("NOTICIA|"):
            parts = line.split("|", 3)
            if len(parts) == 4 and self.news_handler:
                self.news_handler(parts[1], parts[2], parts[3])
        elif line.startswith("INFO"):
            if self.info_handler:
                self.info_handler(line[5:])
        else:
            request_id, separator, response = line.partition(" ")
            if separator and request_id.isdigit():
                self._responses.put((int(request_id), response))
            else:
                self._responses.put((None, line))

    # comandos do protocolo
    def send_command(self, command, timeout=1.5):
        """envia um comando e aguarda uma resposta com o mesmo id"""
        with self._send_lock:
            self._request_id += 1
            request_id = self._request_id
            message = f"{request_id} {command}".encode("utf-8")

            for _ in range(4):
                self.sock.send(message)
                deadline = time.monotonic() + timeout
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        response_id, response = self._responses.get(timeout=remaining)
                    except queue.Empty:
                        break
                    if response_id == request_id:
                        return response

            return "ERRO sem resposta do servidor"

    def subscribe(self, categories):
        return self.send_command("INSCREVER " + ", ".join(categories))

    def unsubscribe(self, categories):
        return self.send_command("CANCELAR " + ", ".join(categories))

    def list_categories(self):
        return self.send_command("CATEGORIAS")

    def my_subscriptions(self):

        return self.send_command("MINHAS")

    def publish(self, category, title, summary):
        return self.send_command(f"PUBLICAR {category}|{title}|{summary}")

    def add_category(self, categories):
        return self.send_command("ADICIONAR " + ", ".join(categories))

    def remove_category(self, categories):
        return self.send_command("REMOVER " + ", ".join(categories))

    def connections(self):
        return self.send_command("CONEXOES")

    def restore_database(self):
        return self.send_command("RESTAURAR")

    def quit(self):
        try:
            return self.send_command("SAIR", timeout=2)
        finally:
            self.stop()