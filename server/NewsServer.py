import threading
import socket
import argparse
import queue
import time

from server.NewsEngine import NewsEngine

# maior datagrama que o servidor le de uma vez
TAMANHO_LEITURA = 65535
# segundos que o recvfrom espera antes de voltar para o laco
ESPERA_LEITURA = 1.0

class ClienteSessao:
    """sessao de um cliente, identificada pelo endereco (ip, porta) de onde vem os datagramas"""
    def __init__(self, servidor_socket, endereco):
        self.socket = servidor_socket   # o socket e um so, o do servidor
        self.addr = endereco
        self.subscriptions = set()  # categorias assinadas por este cliente
        self.thread = None   # thread que atende somente este cliente
        self.fila = queue.Queue()   # datagramas que chegaram deste cliente
        self.ultimo_contato = time.time()
        self.id_atual = 0   # id do comando que esta sendo atendido
        self.enviarlock = threading.Lock()

    def send(self, message): # envia um datagrama para o cliente
        try:
            with self.enviarlock:
                self.socket.sendto(message.encode("utf-8"), self.addr)
            return True
        except OSError:
            return False

    def responder(self, message): # resposta do comando atual, com o id na frente
        return self.send(f"{self.id_atual} {message}")

    def close(self): # avisa a thread da sessao para parar
        self.fila.put(None)

class NewsServer:
    def __init__(self, host="0.0.0.0", port=5000,
                 armazenamento="noticias.jsonl"):
        self.host = host
        self.port = port
        self.engine = NewsEngine(armazenamento)
        self.servidor_socket = None
        self.sessoes = {}   # endereco -> sessao
        self.lock_sessoes = threading.Lock()


    def start(self):
        # sem SO_REUSEADDR: em udp no windows ele deixa dois servidores na mesma porta
        self.servidor_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.servidor_socket.bind((self.host, self.port))
        # o timeout deixa o laco voltar de vez em quando (ctrl+c e limpeza)
        self.servidor_socket.settimeout(ESPERA_LEITURA)
        print(f"[NewsServer] Ouvindo em {self.host}:{self.port} (udp) "
              f"({self.engine.total_news} noticias no historico)", flush=True)

        try:
            while True:
                try:
                    dados, addr = self.servidor_socket.recvfrom(TAMANHO_LEITURA)
                except socket.timeout:
                    self._varrer_expiradas()
                    continue
                except ConnectionResetError:
                    # no windows, mandar datagrama para um cliente que fechou faz o
                    # proximo recvfrom dar erro 10054. e so um aviso, o servidor segue
                    continue
                self._entregar(dados, addr)
        except KeyboardInterrupt:
            print("\n[NewsServer] Encerrando...")
        finally:
            self.stop()

    def stop(self):
        with self.lock_sessoes:
            sessoes = list(self.sessoes.values())
        for sessao in sessoes:
            sessao.close()
        if self.servidor_socket:
            self.servidor_socket.close()
            self.servidor_socket = None
        print("[NewsServer] Servidor parado.")

    def _entregar(self, dados, addr):
        """coloca o datagrama na fila da sessao do endereco, criando a sessao se for nova"""
        with self.lock_sessoes:
            sessao = self.sessoes.get(addr)
            nova = sessao is None
            if nova:
                sessao = ClienteSessao(self.servidor_socket, addr)
                # uma thread dedicada para cada cliente
                sessao.thread = threading.Thread(target=self.handle_client, args=(sessao,),
                                                 daemon=True, name=f"cliente-{addr[0]}:{addr[1]}")
                self.sessoes[addr] = sessao
        sessao.ultimo_contato = time.time()
        sessao.fila.put(dados)
        if nova:
            sessao.thread.start()

    def _remover_sessao(self, sessao):
        with self.lock_sessoes:
            if self.sessoes.get(sessao.addr) is sessao:
                del self.sessoes[sessao.addr]
        self.engine.nao_registrar_sessao(sessao)

    def _varrer_expiradas(self):
        """tira as sessoes que pararam de mandar datagramas"""
        pass

    def handle_client(self, sessao):
        self.engine.registrar_sessao(sessao)
        print(f"[NewsServer] Cliente conectado: {sessao.addr} "
              f"| thread: {threading.current_thread().name} "
              f"(id={threading.get_ident()})", flush=True)
        try:
            sessao.send("INFO Bem-vindo! Categorias: " + ", ".join(self.engine.listar_categorias())
                         + ". Use INSCREVER <categoria>.")
            while True:
                dados = sessao.fila.get()
                if dados is None:   # sessao encerrada
                    break
                try:
                    texto = dados.decode("utf-8").strip()
                except UnicodeDecodeError:
                    continue
                if not texto:
                    continue
                if not self._despachar(sessao, texto):
                    break
        finally:
            self._remover_sessao(sessao)
            print(f"[NewsServer] Cliente desconectado: {sessao.addr}", flush=True)

    def _despachar(self, sessao, texto):
        """atende um datagrama da sessao. retorna False quando a sessao deve ser encerrada"""
        # formato: "<id> COMANDO args", a resposta volta com o mesmo id
        id_texto, _, comando = texto.partition(" ")
        if id_texto.isdigit():
            sessao.id_atual = int(id_texto)
        else:
            sessao.id_atual = 0
            comando = texto
        return self.process_command(sessao, comando)
    
    @staticmethod
    def _parse_categories(text):
        return [c.strip().lower() for c in text.replace(",", " ").split() if c.strip()]

    @staticmethod
    def _clean(text):
        return text.replace("|", "/").replace("\n", " ").replace("\r", " ").strip()

    def process_command(self, session, line):
        """Retorna False quando a sessao deve ser encerrada."""
        command, _, rest = line.partition(" ")
        command = command.upper()

        if command == "CONECTAR":
            # a sessao ja foi criada quando o datagrama chegou
            session.responder("OK conectado")

        elif command == "INSCREVER":
            cats = self._parse_categories(rest)
            if not cats:
                session.responder("ERRO uso: INSCREVER <categoria>[, <categoria>...]")
                return True
            valid, invalid = self.engine.assinar(session, cats)
            msg = "OK inscrito em: " + (", ".join(valid) or "(nenhuma)")
            if invalid:
                msg += " | categorias inexistentes: " + ", ".join(invalid)
            session.responder(msg)

        elif command == "CANCELAR":
            cats = self._parse_categories(rest)
            if not cats:
                session.responder("ERRO uso: CANCELAR <categoria>[, <categoria>...]")
                return True
            removed = self.engine.nao_assinar(session, cats)
            session.responder("OK cancelado: " + (", ".join(removed) or "(nenhuma)"))

        elif command == "ADICIONAR":
            cats = self._parse_categories(rest)
            if not cats:
                session.responder("ERRO uso: ADICIONAR <categoria>[, <categoria>...]")
                return True
            adicionadas, existentes, invalidas = self.engine.adicionar_categorias(cats)
            msg = "OK categoria adicionada ao sistema: " + (", ".join(adicionadas) or "(nenhuma)")
            if existentes:
                msg += " | ja existiam: " + ", ".join(existentes)
            if invalidas:
                msg += " | nomes invalidos: " + ", ".join(invalidas)
            session.responder(msg)

        elif command == "REMOVER":
            cats = self._parse_categories(rest)
            if not cats:
                session.responder("ERRO uso: REMOVER <categoria>[, <categoria>...]")
                return True
            removidas, inexistentes = self.engine.remover_categorias(cats)
            msg = "OK categoria removida do sistema: " + (", ".join(removidas) or "(nenhuma)")
            if inexistentes:
                msg += " | categorias inexistentes: " + ", ".join(inexistentes)
            session.responder(msg)

        elif command == "CATEGORIAS":
            session.responder("OK " + ", ".join(self.engine.listar_categorias()))

        elif command == "MINHAS":
            current = self.engine.assinaturas_da_sessao(session)
            session.responder("OK " + (", ".join(current) or "(nenhuma assinatura)"))

        elif command == "PUBLICAR":
            parts = [self._clean(p) for p in rest.split("|")]
            if len(parts) != 3 or not all(parts):
                session.responder("ERRO uso: PUBLICAR categoria|titulo|resumo")
                return True
            category, title, summary = parts
            category = category.lower()
            categorias = self.engine.listar_categorias()
            if category not in categorias:
                session.responder("ERRO categoria inexistente. Disponiveis: " + ", ".join(categorias))
                return True
            delivered = self.engine.publicar(category, title, summary)
            session.responder(f"OK publicada para {delivered} assinante(s)")

        elif command == "CONEXOES":
            sessoes = self.engine.listar_sessoes()
            ids = {s.thread.ident for s in sessoes}
            linhas = []
            for s in sessoes:
                voce = " (voce)" if s is session else ""
                linhas.append(f"{s.addr[0]}:{s.addr[1]}{voce} -> thread {s.thread.name} "
                              f"id={s.thread.ident} viva={s.thread.is_alive()}")
            session.responder(f"OK {len(sessoes)} conexao(oes) simultanea(s), "
                         f"{len(ids)} thread(s) de cliente distinta(s), "
                         f"{threading.active_count()} thread(s) no processo "
                         f"(1 principal + {len(ids)} de cliente) | " + " | ".join(linhas))

        elif command == "RESTAURAR":
            apagadas = self.engine.restaurar()
            session.responder(f"OK banco restaurado: {apagadas} noticia(s) apagada(s) | "
                         "categorias: " + ", ".join(self.engine.listar_categorias()))

        elif command == "SAIR":
            session.responder("OK ate logo")
            return False

        else:
            session.responder("ERRO comando desconhecido")
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Servidor de feed de noticias PUB/SUB (TCP)")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--arquivo", default="noticias.jsonl")
    args = parser.parse_args()

    NewsServer(args.host, args.port, args.arquivo).start()