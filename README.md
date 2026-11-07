# Feed de Notícias PUB/SUB com Sockets UDP

## Sumário

- [Sobre](#sobre)
- [Como funciona](#como-funciona)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Primeiros passos](#primeiros-passos)
- [Uso](#uso)
- [Comandos do protocolo](#comandos-do-protocolo)
- [Armazenamento](#armazenamento)
- [Testes](#testes)
- [Limitações conhecidas](#limitacoes-conhecidas)
- [Tecnologias utilizadas](#tecnologias-utilizadas)

## Sobre

Este projeto é um sistema distribuído cliente-servidor que funciona como um feed de notícias contínuo, no modelo publicador e assinante (PUB/SUB). A comunicação foi desenvolvida utilizando sockets UDP puros em Python, sem o uso de brokers ou bibliotecas externas.

O servidor mantém a gestão de categorias de notícias, com padrão inicial em tecnologia, esportes e cultura, além de armazenar o histórico de notícias publicadas em arquivo local. Os clientes realizam o envio de datagramas para registrar sessões, assinar categorias de interesse e publicar novas notícias. Quando uma notícia é publicada, o servidor realiza o envio da mensagem diretamente aos assinantes daquela categoria.

Existe um único tipo de usuário no sistema. Portanto, qualquer cliente conectado pode assinar temas, receber atualizações em tempo real, publicar notícias e gerenciar as categorias ativas.

O projeto foi desenvolvido para a atividade APS 02 (Grupo 04). A arquitetura foi adaptada do modelo cliente-servidor original para operar sobre o protocolo UDP com controle de confiabilidade na camada de aplicação.

## Como funciona

O servidor escuta datagramas UDP na porta configurada e cria uma thread dedicada para atender cada sessão de cliente identificada pelo endereço (IP e porta). Dessa forma, o atendimento às requisições ocorre de forma paralela.

Cada sessão guarda o endereço do cliente, as categorias assinadas, a fila de datagramas recebidos e o horário do último contato. O servidor valida o limite de 8192 bytes por datagrama. Mensagens que excedem esse tamanho recebem a resposta de erro correspondente.

A confiabilidade dos comandos é garantida por meio de identificadores numéricos crescentes (<id>) enviados pelo cliente. O servidor armazena o último identificador e a última resposta enviada para cada sessão. Caso receba uma mensagem duplicada com o mesmo identificador, o servidor reenvia a resposta armazenada no cache sem reexecutar o comando.

A manutenção da sessão ocorre através de mensagens de PING enviadas periodicamente pelo cliente a cada 10 segundos. O servidor varre a lista de sessões e remove os clientes que permanecem inativos por mais tempo do que o limite definido no parâmetro de timeout (30 segundos por padrão).

As notificações não solicitadas enviadas pelo servidor possuem os formatos abaixo:
Mensagens que o servidor envia sem ninguem pedir:

```
NOTICIA|categoria|titulo|resumo
INFO texto de aviso
```

As respostas aos comandos executados retornam prefixadas pelo identificador da requisição e pela situação da operação (`<id> OK ...` ou `<id> ERRO ...`).

## Estrutura do projeto

```
aps02-grupo04/
|-- server/
|   |-- NewsServer.py     # socket udp de escuta, sessoes e comandos
|   -- NewsEngine.py     # regras de negocio, sessoes, assinaturas e armazenamento |-- client/ |   |-- NewsClient.py     # biblioteca do cliente, socket udp, reenvio e ping |   -- usuario.py        # interface de linha de comando do usuario
|-- teste/
|   -- teste.py          # testes automatizados de integracao sobre udp -- README.md
```

Dois arquivos são criados pelo servidor durante a execução: `noticias.jsonl` e `categorias.json`.

## Primeiros passos

Estas instruções mostram como executar o projeto em seu ambiente local.

Primeiro, faça o clone do repositório e acesse a pasta do projeto:

```bash
git clone <url-do-repositorio>
cd aps02-grupo04
```

### Pre-requisitos

Basta ter o Python 3 instalado. O projeto usa apenas a biblioteca padrao, entao nao ha nada para instalar com o pip. Foi testado com Python 3.13 em Linux.

```bash
# Debian/Ubuntu
sudo apt install python3
```

No Windows, instale pelo site oficial (https://www.python.org/) e use `python` no lugar de `python3` nos comandos abaixo.

## Uso

### Rodando o servidor

Na raiz do projeto:

```bash
python3 -m server.NewsServer
```

O servidor iniciará a escuta no endereço 0.0.0.0:5000 via UDP. Os parâmetros opcionais de inicialização são os seguintes:

| Parametro   | Padrao            | Descricao                          |
|-------------|-------------------|------------------------------------|
| `--host`    | `0.0.0.0`         | Endereço IP para vinculação do socket do servidor|
| `--port`    | `5000`            | Porta UDP para recepção de datagramas|
| `--arquivo` | `noticias.jsonl`  | Arquivo para persistência do histórico de notícias |
| `--timeout` | `30`  | Tempo em segundos sem contato para expiração de sessão |

Exemplo:

```bash
python3 -m server.NewsServer --port 6000 --arquivo meu_feed.jsonl --timeout 15
```

### Rodando o cliente

Em outro terminal, entre na pasta `client` e abra o usuario:

```bash
cd client
python3 usuario.py
```

Parametros opcionais:

| Parametro      | Padrao      | Descricao                                                       |
|----------------|-------------|-----------------------------------------------------------------|
| `--host`       | `127.0.0.1` | Endereço IP do servidor de notícias                                            |
| `--port`       | `5000`      | Porta UDP do servidor de notícias                                               |
| `--categorias` | vazio       | Categorias para assinar ao conectar, separadas por virgula      |
| `--auto`       | desligado   | Publica noticias de exemplo automaticamente, sem prompt         |
| `--intervalo`  | `5.0`       | Segundos entre as noticias no modo `--auto`                     |

### Exemplo de uso com varios clientes

Abra tres terminais alem do servidor.

Terminal 2, um usuario que quer so tecnologia:

```bash
cd client
python3 usuario.py --categorias tecnologia
```

Terminal 3, um usuario que quer esportes e cultura:

```bash
cd client
python3 usuario.py --categorias esportes,cultura
```

Terminal 4, um usuario que publica noticias de exemplo a cada 2 segundos:

```bash
cd client
python3 usuario.py --auto --intervalo 2
```

Cada usuario recebe na hora apenas as noticias das categorias que assinou. Dentro de um deles, voce tambem pode digitar comandos a qualquer momento, por exemplo:

```
> INSCREVER cultura
OK inscrito em: cultura
> CANCELAR tecnologia
OK cancelado: tecnologia
> PUBLICAR cultura|Festival de cinema abre inscricoes|Mostra recebe filmes ate o fim do mes.
OK publicada para 2 assinante(s)
```

Uma noticia chegando aparece assim:

```
>>> [ESPORTES] Sport vence o classico pernambucano
    Partida terminou 2x1 na Ilha do Retiro.
```

### Ver as conexoes simultaneas

Com varios usuarios conectados, digite `CONEXOES` em qualquer um deles:

```
> CONEXOES
OK 3 conexao(oes) simultanea(s), 3 thread(s) de cliente distinta(s), 4 thread(s) no processo (1 principal + 3 de cliente)
    127.0.0.1:39158 (voce) -> thread cliente-127.0.0.1:39158 id=139792801068736 viva=True
    127.0.0.1:39170 -> thread cliente-127.0.0.1:39170 id=139792792676032 viva=True
    127.0.0.1:39184 -> thread cliente-127.0.0.1:39184 id=139792784283328 viva=True
```

O relatório exibe o número de sessões UDP ativas, os endereços dos clientes e os identificadores das threads associadas a cada atendimento.

## Comandos do protocolo

Os comandos podem ser escritos em maiusculas ou minusculas, e os nomes de categoria tambem. Varias categorias podem ser separadas por virgula ou espaco.

| Comando | O que faz |
|---|---|
| `<id> CONECTAR` | Registra a sessão do cliente no servidor e retorna confirmação de conexão.     |
| `PING` | Mensagem periódica sem identificador enviada pelo cliente para renovar a sessão no servidor. |
| `<id> INSCREVER <cat>[, <cat>...]` | Passa a receber as noticias das categorias informadas. Categorias que nao existem sao listadas na resposta. |
| `<id> CANCELAR <cat>[, <cat>...]` | Cancela a assinatura das categorias informadas. A categoria continua existindo no sistema. |
| `<id> CATEGORIAS` | Lista as categorias que existem no sistema. |
| `<id> MINHAS` | Lista as categorias que voce assina. |
| `<id> PUBLICAR categoria\|titulo\|resumo` | Cadastra uma noticia e a envia na hora para todos os assinantes da categoria. |
| `<id> ADICIONAR <cat>[, <cat>...]` | Cria categorias no sistema de forma permanente. |
| `<id> REMOVER <cat>[, <cat>...]` | Apaga categorias do sistema de forma permanente. |
| `<id> CONEXOES` | Mostra as conexoes simultaneas e a thread que atende cada cliente. |
| `<id> RESTAURAR` | Restaura o banco de dados ao estado original. |
| `<id> SAIR` | Encerra a sessao. |

Detalhes que vale saber:

- As respostas do servidor são retornadas no formato `<id> OK ...` ou `<id> ERRO ...`, mantendo o mesmo identificador da requisição original.
- O cliente realiza até 3 reenvios de um comando caso não receba resposta no intervalo de 1,5 segundos.
- O servidor ignora reexecuções de comandos com identificadores repetidos da mesma sessão, reenviando a resposta salva em cache.
- `CANCELAR` e `REMOVER` sao coisas diferentes. `CANCELAR` mexe so na sua assinatura. `REMOVER` apaga a categoria para todo mundo.
- `ADICIONAR` e `REMOVER` sao permanentes. A mudanca e gravada em `categorias.json` e continua valendo depois de reiniciar o servidor.
- O tamanho máximo permitido para o payload de um datagrama é de 8192 bytes.
- Ao remover uma categoria, todos que a assinavam perdem a assinatura. Nao e possivel publicar em uma categoria que nao existe.
- Quando uma categoria e adicionada ou removida, todos os clientes conectados recebem uma mensagem `INFO` avisando.
- Nomes de categoria podem ter letras, numeros, hifen e underline. O caractere `|` nao e aceito.
- `RESTAURAR` apaga todas as noticias salvas e volta as categorias para tecnologia, esportes e cultura. Como existe um unico tipo de usuario, qualquer cliente pode usar esse comando, entao use com cuidado.
- No `PUBLICAR`, o titulo e o resumo nao podem conter o caractere `|`, porque ele e o separador do comando. Usar `|` a mais faz o servidor responder com o erro de uso.

## Armazenamento

O servidor cria dois arquivos na pasta em que e executado (ou na pasta do arquivo informado em `--arquivo`):

- `noticias.jsonl`: uma noticia por linha, em JSON, no formato abaixo. O texto e gravado em UTF-8, entao os acentos ficam legiveis.

  ```json
  {"ts": "2026-10-30 22:45:10", "categoria": "tecnologia", "titulo": "Nova versao do kernel Linux lancada", "resumo": "Release traz melhorias de desempenho e drivers."}
  ```

- `categorias.json`: a lista de categorias do sistema, por exemplo `["tecnologia", "esportes", "cultura"]`. Se o arquivo nao existir, ele e criado com as tres categorias padrao.

As noticias de uma categoria removida continuam no `noticias.jsonl` como historico. Para zerar tudo, use o comando `RESTAURAR` ou apague os dois arquivos com o servidor parado.

## Testes

A suíte de testes automatizados realiza a validação de integração da aplicação subindo instâncias isoladas do servidor em portas temporárias. Para executar os testes, utilize o comando na raiz do repositório:

```bash
python3 teste/teste.py
```

Saída esperada após a conclusão de todos os cenários:

```
63 ok, 0 falha(s)
```

O que e verificado:

- estabelecimento de sessão UDP e recepção de mensagem de boas-vindas.
- conexao, mensagem de boas-vindas e encerramento da sessao;
- `INSCREVER` e `CANCELAR`, inclusive com categoria inexistente e troca de assinatura no meio da sessao;
- entrega da noticia apenas para quem assinou a categoria, com acentos preservados;
- mensagens de erro do protocolo e continuidade da sessao depois de um erro;
- 10 clientes ao mesmo tempo recebendo a mesma noticia e 3 publicadores enviando em paralelo;
- Limite de tamanho de datagrama de 8192 bytes.
- Expiração de sessões inativas por timeout na ausência de mensagens de PING.
- `RESTAURAR`, `ADICIONAR` e `REMOVER`, incluindo a conferencia de que as categorias continuam como estavam depois de reiniciar o servidor.

## Limitacoes conhecidas

- Nao ha autenticacao. Qualquer pessoa que alcance a porta do servidor pode publicar, mudar categorias e restaurar o banco.
- Notificações push via UDP não garantem entrega porque o protocolo não confirma o recebimento.
- O servidor usa uma thread por cliente, o que funciona bem para dezenas de conexoes, mas nao foi pensado para milhares.
- Um cliente que desconecta perde as assinaturas. Ao voltar, precisa assinar de novo (ou usar `--categorias`).

## Tecnologias utilizadas

Python 3 - linguagem de programação
socket - comunicação UDP via datagramas (SOCK_DGRAM)
threading - gerenciamento de concorrência por sessão e sincronização via locks
JSON Lines - formato para persistência de dados
