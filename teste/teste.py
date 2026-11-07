import os
import sys
import json
import time
import queue
import socket
import argparse
import threading
import tempfile
import subprocess

# raiz do projeto, um nivel acima da pasta teste
RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# deixa importar o NewsClient direto da pasta client
sys.path.insert(0, os.path.join(RAIZ, "client"))

from NewsClient import NewsClient

HOST = "127.0.0.1"
# tempo maximo esperando uma noticia que deveria chegar
ESPERA_NOTICIA = 2.0
# tempo esperando uma noticia que nao deveria chegar
ESPERA_CURTA = 0.5
# tempo para o servidor remover a sessao depois do SAIR
ESPERA_SAIDA = 0.2

acertos = 0
falhas = 0


def checar(descricao, condicao, detalhe=""):
    global acertos, falhas
    if condicao:
        acertos += 1
        print(f"  [ok]    {descricao}")
    else:
        falhas += 1
        print(f"  [falha] {descricao}")
        if detalhe:
            print(f"          recebido: {detalhe}")


def subir_servidor(porta, arquivo, log, timeout=30):
    # roda o servidor como modulo a partir da raiz, igual ao uso normal
    saida = open(log, "w", encoding="utf-8")
    processo = subprocess.Popen(
        [sys.executable, "-m", "server.NewsServer", "--host", HOST,
         "--port", str(porta), "--arquivo", arquivo, "--timeout", str(timeout)],
        cwd=RAIZ, stdout=saida, stderr=subprocess.STDOUT)

    # espera a porta abrir usando UDP: envia 1 CONECTAR e aguarda resposta OK conectado
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(0.5)
    limite = time.time() + 5
    while time.time() < limite:
        if processo.poll() is not None:
            sock.close()
            print("[teste] o servidor encerrou antes de abrir a porta")
            return None
        try:
            sock.sendto(b"1 CONECTAR", (HOST, porta))
            while True:
                dados, _ = sock.recvfrom(8192)
                texto = dados.decode("utf-8")
                if "OK conectado" in texto:
                    sock.sendto(b"2 SAIR", (HOST, porta))
                    sock.close()
                    time.sleep(ESPERA_SAIDA)
                    return processo
        except socket.timeout:
            time.sleep(0.1)
        except OSError:
            time.sleep(0.1)

    sock.close()
    print("[teste] o servidor nao abriu a porta a tempo")
    processo.kill()
    return None


def derrubar_servidor(processo):
    processo.terminate()
    try:
        processo.wait(timeout=3)
    except subprocess.TimeoutExpired:
        processo.kill()


def mostrar_log(log):
    print("\n[teste] saida do servidor:")
    with open(log, encoding="utf-8") as f:
        print(f.read())


def novo_cliente(porta):
    cliente = NewsClient(HOST, porta)
    # guarda as noticias e mensagens INFO que chegarem para conferir depois
    cliente.noticias = queue.Queue()
    cliente.infos = queue.Queue()
    cliente.set_news_handler(lambda cat, tit, res: cliente.noticias.put((cat, tit, res)))
    cliente.set_info_handler(cliente.infos.put)
    cliente.start()
    return cliente


def proxima_noticia(cliente, espera=ESPERA_NOTICIA):
    try:
        return cliente.noticias.get(timeout=espera)
    except queue.Empty:
        return None


def encerrar(*clientes):
    for cliente in clientes:
        cliente.quit()
    # da tempo do servidor tirar as sessoes da lista antes do proximo teste
    time.sleep(ESPERA_SAIDA)


def teste_conexao(porta):
    print("\nconexao")
    cliente = novo_cliente(porta)

    try:
        info = cliente.infos.get(timeout=ESPERA_NOTICIA)
    except queue.Empty:
        info = ""
    checar("recebe a mensagem de boas-vindas", "Bem-vindo" in info, info)

    resposta = cliente.list_categories()
    checar("CATEGORIAS lista as tres categorias", resposta == "OK tecnologia, esportes, cultura", resposta)

    resposta = cliente.quit()
    checar("SAIR encerra a sessao", resposta == "OK ate logo", resposta)
    time.sleep(ESPERA_SAIDA)


def teste_assinaturas(porta):
    print("\ninscricao e cancelamento")
    cliente = novo_cliente(porta)

    resposta = cliente.subscribe(["tecnologia", "esportes"])
    checar("INSCREVER em duas categorias", resposta == "OK inscrito em: tecnologia, esportes", resposta)

    resposta = cliente.my_subscriptions()
    checar("MINHAS mostra as duas", resposta == "OK esportes, tecnologia", resposta)

    resposta = cliente.unsubscribe(["tecnologia"])
    checar("CANCELAR uma categoria", resposta == "OK cancelado: tecnologia", resposta)

    resposta = cliente.my_subscriptions()
    checar("MINHAS depois de cancelar", resposta == "OK esportes", resposta)

    resposta = cliente.unsubscribe(["cultura"])
    checar("CANCELAR categoria que nao estava assinada", resposta == "OK cancelado: (nenhuma)", resposta)

    # a valida entra e a inexistente aparece na resposta
    resposta = cliente.subscribe(["cultura", "politica"])
    checar("INSCREVER com uma categoria inexistente",
           resposta == "OK inscrito em: cultura | categorias inexistentes: politica", resposta)

    resposta = cliente.send_command("inscrever ESPORTES")
    checar("comando e categoria em maiusculas ou minusculas", resposta == "OK inscrito em: esportes", resposta)

    encerrar(cliente)


def teste_entrega(porta):
    print("\npublicacao e entrega")
    leitor_tec = novo_cliente(porta)
    leitor_esp = novo_cliente(porta)
    autor = novo_cliente(porta)
    leitor_tec.subscribe(["tecnologia"])
    leitor_esp.subscribe(["esportes"])

    noticia = ("tecnologia", "Novo processador lancado", "Chip promete gastar menos energia.")
    resposta = autor.publish(*noticia)
    checar("PUBLICAR confirma 1 assinante", resposta == "OK publicada para 1 assinante(s)", resposta)
    recebida = proxima_noticia(leitor_tec)
    checar("assinante de tecnologia recebe categoria, titulo e resumo", recebida == noticia, recebida)
    recebida = proxima_noticia(leitor_esp, ESPERA_CURTA)
    checar("assinante de esportes nao recebe", recebida is None, recebida)
    recebida = proxima_noticia(autor, ESPERA_CURTA)
    checar("autor sem assinatura nao recebe", recebida is None, recebida)

    # troca a assinatura no meio da sessao
    leitor_tec.unsubscribe(["tecnologia"])
    leitor_tec.subscribe(["cultura"])

    autor.publish("tecnologia", "Atualizacao de seguranca", "Correcao para uma falha no navegador.")
    recebida = proxima_noticia(leitor_tec, ESPERA_CURTA)
    checar("depois de CANCELAR nao recebe mais a categoria", recebida is None, recebida)

    # titulo com acento para conferir o utf-8 de ponta a ponta
    noticia = ("cultura", "Exposição de arte popular no Recife", "Entrada gratuita até o fim do mês.")
    autor.publish(*noticia)
    recebida = proxima_noticia(leitor_tec)
    checar("depois de INSCREVER passa a receber a nova categoria", recebida == noticia, recebida)

    encerrar(leitor_tec, leitor_esp, autor)
    # quantidade de noticias publicadas com sucesso neste teste
    return 3


def teste_erros(porta):
    print("\nerros do protocolo")
    cliente = novo_cliente(porta)

    # comando enviado e resposta esperada do servidor
    casos = [
        ("INSCREVER", "ERRO uso: INSCREVER <categoria>[, <categoria>...]"),
        ("CANCELAR", "ERRO uso: CANCELAR <categoria>[, <categoria>...]"),
        ("PUBLICAR esportes|so o titulo", "ERRO uso: PUBLICAR categoria|titulo|resumo"),
        ("PUBLICAR esportes||resumo sem titulo", "ERRO uso: PUBLICAR categoria|titulo|resumo"),
        ("PUBLICAR politica|Titulo|Resumo", "ERRO categoria inexistente. Disponiveis: tecnologia, esportes, cultura"),
        ("LISTAR", "ERRO comando desconhecido"),
    ]
    for comando, esperado in casos:
        resposta = cliente.send_command(comando)
        checar(f"'{comando}' retorna erro", resposta == esperado, resposta)

    # depois dos erros a sessao continua funcionando
    resposta = cliente.list_categories()
    checar("sessao continua aberta depois dos erros", resposta.startswith("OK"), resposta)

    encerrar(cliente)


def teste_simultaneos(porta, quantidade=10):
    print(f"\n{quantidade} clientes ao mesmo tempo")
    leitores = [novo_cliente(porta) for _ in range(quantidade)]
    for leitor in leitores:
        leitor.subscribe(["esportes"])
    autor = novo_cliente(porta)

    resposta = autor.publish("esportes", "Final do campeonato", "Jogo decidido nos penaltis.")
    checar(f"PUBLICAR confirma {quantidade} assinantes",
           resposta == f"OK publicada para {quantidade} assinante(s)", resposta)
    recebidas = sum(1 for leitor in leitores if proxima_noticia(leitor) is not None)
    checar("todos os clientes recebem a noticia", recebidas == quantidade, f"{recebidas} de {quantidade}")

    # tres autores publicando juntos, cada um em uma thread
    autores = [novo_cliente(porta) for _ in range(3)]
    threads = [
        threading.Thread(target=e.publish, args=("esportes", f"Rodada {i}", f"Resumo da rodada {i}."))
        for i, e in enumerate(autores)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # cada leitor deve receber as tres, em qualquer ordem
    completos = 0
    for leitor in leitores:
        titulos = set()
        for _ in range(3):
            noticia = proxima_noticia(leitor)
            if noticia is None:
                break
            titulos.add(noticia[1])
        if titulos == {"Rodada 0", "Rodada 1", "Rodada 2"}:
            completos += 1
    checar("publicacoes simultaneas chegam inteiras para todos",
           completos == quantidade, f"{completos} de {quantidade}")

    encerrar(*leitores, autor, *autores)
    return 1 + len(autores)


def teste_armazenamento(arquivo, esperadas):
    print("\narmazenamento")
    try:
        with open(arquivo, encoding="utf-8") as f:
            registros = [json.loads(linha) for linha in f if linha.strip()]
    except (OSError, json.JSONDecodeError) as erro:
        checar("arquivo de noticias pode ser lido", False, erro)
        return

    checar(f"arquivo tem as {esperadas} noticias publicadas", len(registros) == esperadas,
           f"{len(registros)} registros")
    campos = {"ts", "categoria", "titulo", "resumo"}
    checar("todo registro tem ts, categoria, titulo e resumo",
           all(set(r) == campos for r in registros))
    # o acento precisa ficar legivel no arquivo, sem virar ç
    titulos = [r.get("titulo") for r in registros]
    checar("acentos gravados corretamente", "Exposição de arte popular no Recife" in titulos)

def esperar_info(cliente, trecho, espera=ESPERA_NOTICIA):
    """Espera chegar uma mensagem INFO que contenha o trecho."""
    limite = time.time() + espera
    while time.time() < limite:
        try:
            info = cliente.infos.get(timeout=0.2)
        except queue.Empty:
            continue
        if trecho in info:
            return info
    return None


class ServidorProprio:
    """Servidor com arquivos temporarios proprios, para os testes que alteram
    o banco (categorias e noticias) sem atrapalhar os outros testes."""

    def __init__(self, porta, timeout=30):
        self.porta = porta
        self.timeout = timeout
        pasta = tempfile.mkdtemp(prefix="feed_banco_")
        self.arquivo = os.path.join(pasta, "noticias.jsonl")
        self.log = os.path.join(pasta, "servidor.log")
        self.processo = None

    def subir(self):
        self.processo = subir_servidor(self.porta, self.arquivo, self.log, timeout=self.timeout)
        return self.processo is not None

    def derrubar(self):
        if self.processo:
            derrubar_servidor(self.processo)
            self.processo = None

    def total_noticias(self):
        with open(self.arquivo, encoding="utf-8") as f:
            return sum(1 for linha in f if linha.strip())


def teste_restaurar(porta):
    print("\nRESTAURAR o banco de dados")
    servidor = ServidorProprio(porta)
    if not servidor.subir():
        checar("servidor proprio subiu", False)
        return
    try:
        cliente = novo_cliente(porta)
        cliente.publish("tecnologia", "Noticia antiga", "Sera apagada.")
        checar("arquivo tem 1 noticia antes de restaurar", servidor.total_noticias() == 1)

        resposta = cliente.restore_database()
        checar("RESTAURAR responde com o que foi apagado",
               resposta == "OK banco restaurado: 1 noticia(s) apagada(s) | categorias: tecnologia, esportes, cultura",
               resposta)
        checar("arquivo de noticias fica vazio", servidor.total_noticias() == 0)
        encerrar(cliente)
    finally:
        servidor.derrubar()

def teste_remover_categoria(porta):
    print("\nREMOVER categoria de vez")
    servidor = ServidorProprio(porta)
    if not servidor.subir():
        checar("servidor proprio subiu", False)
        return
    try:
        leitor = novo_cliente(porta)
        autor = novo_cliente(porta)
        leitor.subscribe(["esportes", "cultura"])

        resposta = autor.remove_category(["esportes"])
        checar("REMOVER apaga a categoria do sistema",
               resposta == "OK categoria removida do sistema: esportes", resposta)
        resposta = autor.list_categories()
        checar("CATEGORIAS nao lista mais a removida", resposta == "OK tecnologia, cultura", resposta)
        resposta = leitor.my_subscriptions()
        checar("quem assinava perde a assinatura", resposta == "OK cultura", resposta)
        aviso = esperar_info(leitor, "categoria removida do sistema: esportes")
        checar("clientes conectados recebem o aviso", aviso is not None)
        resposta = autor.publish("esportes", "Titulo", "Resumo")
        checar("PUBLICAR na categoria removida da erro",
               resposta == "ERRO categoria inexistente. Disponiveis: tecnologia, cultura", resposta)
        resposta = autor.remove_category(["politica"])
        checar("REMOVER categoria que nao existe",
               resposta == "OK categoria removida do sistema: (nenhuma) | categorias inexistentes: politica", resposta)
        resposta = autor.send_command("REMOVER")
        checar("REMOVER sem argumento retorna erro",
               resposta == "ERRO uso: REMOVER <categoria>[, <categoria>...]", resposta)
        encerrar(leitor, autor)

        # "de vez": reinicia o servidor e a categoria continua apagada
        servidor.derrubar()
        servidor.subir()
        cliente = novo_cliente(porta)
        resposta = cliente.list_categories()
        checar("depois de reiniciar o servidor continua removida", resposta == "OK tecnologia, cultura", resposta)

        resposta = cliente.restore_database()
        checar("RESTAURAR traz de volta a categoria removida",
               resposta.endswith("categorias: tecnologia, esportes, cultura"), resposta)
        encerrar(cliente)
    finally:
        servidor.derrubar()

def teste_adicionar_categoria(porta):
    print("\nADICIONAR categoria de vez")
    servidor = ServidorProprio(porta)
    if not servidor.subir():
        checar("servidor proprio subiu", False)
        return
    try:
        leitor = novo_cliente(porta)
        autor = novo_cliente(porta)

        resposta = autor.add_category(["politica"])
        checar("ADICIONAR cria a categoria",
               resposta == "OK categoria adicionada ao sistema: politica", resposta)
        resposta = autor.list_categories()
        checar("CATEGORIAS lista a nova", resposta == "OK tecnologia, esportes, cultura, politica", resposta)
        aviso = esperar_info(leitor, "nova categoria disponivel: politica")
        checar("clientes conectados recebem o aviso", aviso is not None)

        # a categoria nova funciona como qualquer outra
        resposta = leitor.subscribe(["politica"])
        checar("INSCREVER na categoria nova", resposta == "OK inscrito em: politica", resposta)
        noticia = ("politica", "Votacao adiada", "Sessao remarcada para a proxima semana.")
        autor.publish(*noticia)
        recebida = proxima_noticia(leitor)
        checar("assinante recebe noticia da categoria nova", recebida == noticia, recebida)

        resposta = autor.add_category(["Politica"])
        checar("ADICIONAR categoria que ja existe",
               resposta == "OK categoria adicionada ao sistema: (nenhuma) | ja existiam: politica", resposta)
        resposta = autor.add_category(["a|b"])
        checar("ADICIONAR com nome invalido",
               resposta == "OK categoria adicionada ao sistema: (nenhuma) | nomes invalidos: a|b", resposta)
        resposta = autor.send_command("ADICIONAR")
        checar("ADICIONAR sem argumento retorna erro",
               resposta == "ERRO uso: ADICIONAR <categoria>[, <categoria>...]", resposta)
        encerrar(leitor, autor)

        # "de vez": reinicia o servidor e a categoria continua existindo
        servidor.derrubar()
        servidor.subir()
        cliente = novo_cliente(porta)
        resposta = cliente.list_categories()
        checar("depois de reiniciar o servidor continua existindo",
               resposta == "OK tecnologia, esportes, cultura, politica", resposta)

        resposta = cliente.restore_database()
        checar("RESTAURAR apaga a categoria adicionada",
               resposta.endswith("categorias: tecnologia, esportes, cultura"), resposta)
        encerrar(cliente)
    finally:
        servidor.derrubar()

def teste_conexoes(porta, quantidade=5):
    print(f"\nCONEXOES com {quantidade} clientes")
    clientes = [novo_cliente(porta) for _ in range(quantidade)]
    # a mensagem de boas-vindas so sai depois que o servidor registrou a sessao
    for c in clientes:
        c.infos.get(timeout=ESPERA_NOTICIA)

    resposta = clientes[0].connections()
    partes = resposta.split(" | ")
    resumo = partes[0]
    checar(f"mostra {quantidade} conexoes simultaneas",
           resumo.startswith(f"OK {quantidade} conexao(oes) simultanea(s)"), resumo)
    checar(f"mostra {quantidade} threads de cliente distintas",
           f"{quantidade} thread(s) de cliente distinta(s)" in resumo, resumo)
    checar(f"processo tem 1 thread principal + {quantidade} de cliente",
           f"{quantidade + 1} thread(s) no processo" in resumo, resumo)
    ids = [p.split("id=")[1].split()[0] for p in partes[1:]]
    checar("cada conexao aparece com um id de thread diferente",
           len(ids) == quantidade and len(set(ids)) == quantidade, ids)
    checar("quem pediu aparece marcado com (voce)", resposta.count("(voce)") == 1)

    # quando os clientes saem, as threads deles terminam e saem da lista
    encerrar(*clientes)
    novo = novo_cliente(porta)
    novo.infos.get(timeout=ESPERA_NOTICIA)
    resumo = novo.connections().split(" | ")[0]
    checar("depois que saem so resta 1 conexao",
           resumo.startswith("OK 1 conexao(oes) simultanea(s)"), resumo)
    encerrar(novo)

# Novos testes exigidos para a Pessoa 5
def _recv_ignoring_info(sock, timeout=2.0):
    limite = time.time() + timeout
    while time.time() < limite:
        try:
            dados, _ = sock.recvfrom(8192)
            texto = dados.decode("utf-8", errors="ignore")
            if texto.startswith("INFO "):
                continue
            return texto
        except socket.timeout:
            break
    return ""

def teste_id_duplicado(porta):
    print("\nID duplicado (socket cru)")
    servidor = ServidorProprio(porta)
    if not servidor.subir():
        checar("servidor proprio subiu", False)
        return
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(2.0)
        addr = (HOST, porta)

        # Primeiro CONECTAR
        sock.sendto(b"1 CONECTAR", addr)
        resp1 = _recv_ignoring_info(sock)
        checar("CONECTAR responde OK", "1 OK conectado" in resp1, resp1)

        # PUBLICAR com id 2
        cmd = b"2 PUBLICAR tecnologia|Titulo Dup|Resumo Dup"
        sock.sendto(cmd, addr)
        resp2 = _recv_ignoring_info(sock)
        checar("PUBLICAR responde OK", "2 OK publicada" in resp2, resp2)

        # Reenviar exatamente o mesmo ID 2
        sock.sendto(cmd, addr)
        resp3 = _recv_ignoring_info(sock)
        checar("reenvio responde a mesma mensagem gravada", resp2 == resp3, resp3)

        # Conferir que no arquivo so gravou 1 noticia
        total = servidor.total_noticias()
        checar("mesmo id enviado 2 vezes grava 1 noticia so", total == 1, f"total={total}")

        sock.sendto(b"3 SAIR", addr)
        sock.close()
    finally:
        servidor.derrubar()

def teste_timeout_expiracao(porta):
    print("\nexpiracao de sessao por timeout")
    servidor = ServidorProprio(porta, timeout=2)
    if not servidor.subir():
        checar("servidor proprio subiu", False)
        return
    try:
        # Cliente 1 via socket cru (sem PING)
        sock_parado = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock_parado.sendto(b"1 CONECTAR", (HOST, porta))
        _recv_ignoring_info(sock_parado)

        # Cliente 2 usando NewsClient (com PING ativo)
        cliente_ativo = novo_cliente(porta)
        cliente_ativo.infos.get(timeout=ESPERA_NOTICIA)

        # Espera 3.5 segundos para dar tempo do cliente sem PING expirar (> 2s)
        time.sleep(3.5)

        resposta = cliente_ativo.connections()
        checar("sessao sem PING sumiu do CONEXOES", "1 conexao(oes) simultanea(s)" in resposta, resposta)

        encerrar(cliente_ativo)
        sock_parado.close()
    finally:
        servidor.derrubar()

def teste_tamanho_limite(porta):
    print("\nlimite de tamanho de datagrama (9000 bytes)")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(2.0)
    addr = (HOST, porta)

    # envia datagrama com 9000 bytes
    payload = b"1 PUBLICAR tecnologia|" + (b"A" * 8980)
    sock.sendto(payload, addr)
    texto = _recv_ignoring_info(sock)
    checar("datagrama de 9000 bytes recebe ERRO mensagem muito grande", "ERRO mensagem muito grande" in texto, texto)
    sock.close()

def main():
    parser = argparse.ArgumentParser(description="Testes do feed de noticias PUB/SUB")
    parser.add_argument("--port", type=int, default=5050, help="porta usada pelo servidor de teste")
    args = parser.parse_args()

    # arquivo de noticias e log ficam numa pasta temporaria, sem sujar o projeto
    pasta = tempfile.mkdtemp(prefix="feed_teste_")
    arquivo = os.path.join(pasta, "noticias.jsonl")
    log = os.path.join(pasta, "servidor.log")

    servidor = subir_servidor(args.port, arquivo, log)
    if servidor is None:
        mostrar_log(log)
        sys.exit(1)

    testes = [teste_conexao, teste_assinaturas, teste_entrega, teste_erros, teste_simultaneos, teste_conexoes, teste_tamanho_limite]
    publicadas = 0
    try:
        for teste in testes:
            # uma excecao conta como falha e nao impede os proximos testes
            try:
                publicadas += teste(args.port) or 0
            except Exception as erro:
                checar(f"{teste.__name__} rodou ate o fim", False, repr(erro))
    finally:
        derrubar_servidor(servidor)

    teste_armazenamento(arquivo, publicadas)

    # testes que alteram o banco ou usam configuracoes proprias rodam em servidores proprios
    for teste in (teste_restaurar, teste_remover_categoria, teste_adicionar_categoria, teste_id_duplicado, teste_timeout_expiracao):
        try:
            teste(args.port + 1)
        except Exception as erro:
            checar(f"{teste.__name__} rodou ate o fim", False, repr(erro))

    print(f"\n{acertos} ok, {falhas} falha(s)")
    if falhas:
        mostrar_log(log)
    sys.exit(1 if falhas else 0)


if __name__ == "__main__":
    main()
