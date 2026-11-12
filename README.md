# Trabalho 3: Replicação de dados com gRPC (modelo Pull)

Sistema com **1 cliente + 4 nós** (mesmo código), com eleição de líder estilo Raft,
replicação **pull** estilo KRaft (as réplicas buscam os dados com `FetchRequest`
a cada 100 ms), quórum fixo de 3 nós e leitura eventual em qualquer nó.
Toda comunicação é feita via **gRPC** (Python, `grpcio` + `protobuf`), seguindo o
que foi visto em aula: serviço definido em `.proto`, compilado com `protoc` para
gerar as classes stub, servidor implementando os métodos e cliente usando o stub
(RPCs simples, unárias).

## Arquivos

| Arquivo | Conteúdo |
|---|---|
| `proto/replicacao.proto` | Definição dos serviços e mensagens (IDL protobuf) |
| `gerar_proto.py` | Compila o `.proto` (gera `replicacao_pb2.py` e `replicacao_pb2_grpc.py`) |
| `config.py` | Endereços dos 4 nós, quórum e temporizadores |
| `no.py` | Nó do cluster (seguidor / candidato / líder) |
| `persistencia.py` | Gravação em disco do termo, voto, log e banco final |
| `cliente.py` | Cliente (gravação com redirecionamento, leitura, estado) |

Dois serviços gRPC no `.proto`:

- **ServicoCliente**: `Gravar`, `Ler`, `Estado` (usado pelo cliente).
- **ServicoReplicacao**: `Fetch` e `RequestVote` (usados entre os nós).

## Instalação

Requer Python 3.9+.

```bash
pip install -r requirements.txt
python gerar_proto.py        # opcional: no.py e cliente.py geram sozinhos se faltar
```

O `gerar_proto.py` equivale a
`python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/replicacao.proto`.

## Execução

Abra **5 terminais** na pasta do projeto:

```bash
python no.py --id 1 --limpar     # terminal 1  (--limpar só na primeira vez: apaga dados/no1)
python no.py --id 2 --limpar     # terminal 2
python no.py --id 3 --limpar     # terminal 3
python no.py --id 4 --limpar     # terminal 4
python cliente.py                # terminal 5 (modo interativo)
```

Cada nó imprime em todas as linhas o **papel, termo e índice de commit**, além de um
`STATUS` a cada 3 s. O estado fica salvo em `dados/no<ID>/` (`estado.json`,
`log.json`, `banco.json`). Para reiniciar um nó **mantendo** o estado, rode sem `--limpar`.

Comandos do cliente (no modo interativo, ou direto: `python cliente.py gravar a 1`):

```
gravar <chave> <valor> [--no N] [--crash]   grava; se cair num seguidor, é redirecionado ao líder
ler [<chave>] [--no N]                     lê só dados comitados de um nó
ler-todos [<chave>]                        lê de todos os nós
estado [--no N]                            papel, termo, voto, commit, log (comitada/PENDENTE) e réplicas
```

Para derrubar um nó, use **Ctrl+C** no terminal dele (ou feche o terminal).

## Roteiro dos cenários de teste

**1. Escrita normal e leitura distribuída**
```
gravar a 1 --no 1        # nó 1 é seguidor -> REDIRECIONADO ao líder -> OK comitado
ler-todos a              # o valor aparece nos 4 nós
```

**2. Eleição automática por queda do líder**
`estado` para ver o líder → Ctrl+C no líder. Em 1,5 a 3 s um seguidor mostra
`temporizador expirou ... CANDIDATO no termo T+1`, os outros mostram `VOTO concedido`
e ele imprime `*** ELEITO LÍDER do termo T+1 ***`. Rode `estado` de novo.

**3. Bloqueio por perda de quórum**
Com 3 nós ativos, derrube mais um (ficam 2). O líder imprime
`réplica X INATIVA ... SEM QUÓRUM, escritas bloqueadas`, e:
```
gravar z 9               # FALHOU: sem quórum: 2/4 nós ativos, necessário 3
```
Religue um nó e a escrita volta a funcionar.

**4A. Réplica desatualizada**
Ctrl+C em um seguidor → `gravar b 2`, `gravar c 3` (OK, 3 nós ativos) → religue o
seguidor (`python no.py --id N`, sem `--limpar`). Ele imprime
`Fetch trouxe 2 entrada(s)` e depois os `COMMIT`. Confira com `ler-todos`.

**4B. Truncamento por divergência**
```
gravar x perdido --crash     # o líder (L1) grava no log local e encerra o processo na hora
```
Espere o novo líder (L2, termo maior) e faça `gravar y ok`. Religue L1 **sem** `--limpar`.
L1 imprime `termo maior recebido ... -> SEGUIDOR`,
`TRUNCANDO log: removidas 1 entrada(s) não comitada(s): [..|SET x=perdido]` e depois busca
o log correto via Fetch. `estado --no <L1>` mostra que `x` sumiu e `y` está lá.

**5. Isolamento de dados não comitados**
Reinicie um seguidor com atraso artificial entre os Fetches:
```bash
python no.py --id 4 --atraso-commit 8
```
Depois, no cliente:
```
gravar f 6
ler f --no 4             # (não encontrado / não comitado)
estado --no 4            # a entrada aparece no log como PENDENTE, fora do banco final
# após ~8 s
ler f --no 4             # f = 6
```
A réplica recebe a entrada num Fetch, mas só a efetiva no banco quando envia o
**próximo** Fetch e recebe o índice de commit atualizado.

## Como cada requisito foi implementado (para a defesa)

- **Termo e eleição** (`_loop_eleicao`, `_eleicao`): cada nó tem um temporizador aleatório
  (1,5 a 3 s) reiniciado a cada Fetch bem-sucedido no líder. Ao expirar: `termo += 1`, vira
  CANDIDATO, vota em si e envia `RequestVote` aos outros; com 3 votos vira LÍDER.
- **Atualização obrigatória de termo** (`_atualizar_termo`): qualquer mensagem ou resposta
  com termo maior faz o nó adotar o termo, limpar o voto e virar SEGUIDOR.
- **Voto único e validação do log** (`RequestVote`): no máximo um voto por termo
  (`voted_for`, persistido antes de responder) e só para candidato com log tão ou mais
  atualizado (último termo maior, ou igual com índice maior ou igual).
- **Gravação no líder** (`Gravar`): verifica o quórum de nós ativos, grava no log com o termo
  atual e o próximo índice, e só responde sucesso quando o índice de commit alcança a entrada.
  Seguidor ou candidato devolve o id e endereço do líder.
- **FetchRequest como heartbeat** (`_loop_fetch` na réplica, `Fetch` no líder): a réplica envia
  termo, último índice e último termo a cada 100 ms. O líder registra o instante do Fetch,
  confere a consistência do log e devolve as novas entradas e o índice de commit.
- **Temporizador do líder** (`_loop_monitor_lider`): Heartbeat Timeout de 500 ms por réplica;
  sem Fetch nesse tempo a réplica é marcada INATIVA, e sem quórum as escritas são recusadas.
- **Índice de commit** (`_avancar_commit`): o líder ordena os índices confirmados (o dele e os
  informados nos Fetches) e pega o 3º maior, que é o maior índice presente em 3 nós. Como no
  Raft, só comita por contagem entradas do termo atual; por isso o novo líder grava uma
  entrada `NOOP` ao assumir.
- **Truncamento**: se o último índice/termo da réplica não bate com o log do líder, o líder
  responde `divergente=true` e `truncar_ate`; a réplica apaga as entradas a partir do ponto
  conflitante (`_truncar`) e busca de novo.
- **Leitura eventual** (`Ler`): responde só com o banco final, que contém apenas entradas comitadas.
- **Persistência** (`persistencia.py`): termo e voto, log e banco final, com escrita atômica
  (arquivo temporário + `fsync` + `os.replace`). Ao reiniciar, o nó recarrega tudo e volta
  como SEGUIDOR.
- **Descoberta do líder**: uma réplica que não sabe quem é o líder (ao iniciar ou após uma
  eleição) envia Fetch aos outros nós em rodízio; quem não é líder responde com o id do líder
  que conhece.

## Rodar em máquinas diferentes

Edite `NOS` em `config.py` trocando `localhost` pelos IPs de cada máquina (o mesmo arquivo em todas).
