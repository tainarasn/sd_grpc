"""Configuração do cluster (4 nós) e dos temporizadores."""

# id do nó -> endereço gRPC. Para rodar em máquinas diferentes, troque "localhost" pelos IPs.
NOS = {
    1: "localhost:50051",
    2: "localhost:50052",
    3: "localhost:50053",
    4: "localhost:50054",
}

N = len(NOS)
QUORUM = N // 2 + 1                    # maioria absoluta: 4 nós -> 3

INTERVALO_FETCH = 0.100                # réplica envia FetchRequest a cada 100 ms
HEARTBEAT_TIMEOUT = 0.500              # líder considera a réplica inativa após 500 ms sem Fetch
ELEICAO_TIMEOUT_MIN = 1.5              # temporizador aleatório da réplica (segundos)
ELEICAO_TIMEOUT_MAX = 3.0
TIMEOUT_RPC = 0.300                    # timeout de cada chamada gRPC entre nós
TIMEOUT_COMMIT = 5.0                   # tempo máximo que o líder espera o commit de uma escrita
MAX_ENTRADAS_POR_FETCH = 100
INTERVALO_STATUS = 3.0                 # de quanto em quanto tempo o nó imprime seu estado
