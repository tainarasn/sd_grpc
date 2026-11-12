"""Cliente do cluster.

Modo interativo:
    python cliente.py

Ou um comando direto:
    python cliente.py gravar <chave> <valor> [--no N] [--crash]
    python cliente.py ler [<chave>] [--no N]     (sem chave: todos os dados comitados)
    python cliente.py ler-todos [<chave>]        (consulta os 4 nós)
    python cliente.py estado [--no N]            (sem --no: todos os nós)
"""
import argparse
import shlex
import sys

import gerar_proto
gerar_proto.garantir()

import grpc  # noqa: E402
import replicacao_pb2 as pb  # noqa: E402
import replicacao_pb2_grpc as pb_grpc  # noqa: E402

import config  # noqa: E402

TIMEOUT = 8.0


def stub(no_id):
    return pb_grpc.ServicoClienteStub(grpc.insecure_channel(config.NOS[no_id]))


def gravar(chave, valor, no_inicial=None, simular_crash=False):
    """Envia a escrita a um nó. Se ele não for o líder, segue a indicação de líder."""
    candidatos = [no_inicial] if no_inicial else []
    candidatos += [n for n in config.NOS if n not in candidatos]
    tentados = set()
    alvo = candidatos[0]
    for _ in range(8):
        tentados.add(alvo)
        print(f"-> enviando Gravar({chave}={valor}) ao nó {alvo} ({config.NOS[alvo]})")
        try:
            r = stub(alvo).Gravar(pb.GravarRequest(chave=chave, valor=valor,
                                                   simular_crash=simular_crash), timeout=TIMEOUT)
        except grpc.RpcError as e:
            if simular_crash and e.code() == grpc.StatusCode.UNAVAILABLE and alvo in tentados:
                print(f"   nó {alvo} caiu (crash simulado) depois de gravar no log local.")
                return
            print(f"   nó {alvo} indisponível ({e.code().name})")
            restantes = [n for n in candidatos if n not in tentados]
            if not restantes:
                break
            alvo = restantes[0]
            continue

        if r.sucesso:
            print(f"   OK: {r.mensagem}")
            return
        if r.lider_id and r.lider_id != alvo:
            print(f"   REDIRECIONADO: {r.mensagem} -> reenviando ao líder {r.lider_id} ({r.lider_endereco})")
            alvo = r.lider_id
            continue
        print(f"   FALHOU: {r.mensagem}")
        if r.lider_id == alvo:
            return                                   # o próprio líder recusou (ex.: sem quórum)
        restantes = [n for n in candidatos if n not in tentados]
        if not restantes:
            break
        alvo = restantes[0]
    print("   não foi possível gravar (líder não encontrado/indisponível)")


def ler(chave, no_id):
    try:
        r = stub(no_id).Ler(pb.LerRequest(chave=chave or ""), timeout=TIMEOUT)
    except grpc.RpcError as e:
        print(f"nó {no_id}: indisponível ({e.code().name})")
        return
    cab = f"nó {r.no_id} [{r.papel}, termo {r.termo}, commit {r.indice_commit}]"
    if chave:
        print(f"{cab}: {chave} = {r.valor if r.encontrado else '(não encontrado / não comitado)'}")
    else:
        print(f"{cab}: {dict(sorted(r.dados.items()))}")


def estado(no_id):
    try:
        r = stub(no_id).Estado(pb.EstadoRequest(), timeout=TIMEOUT)
    except grpc.RpcError as e:
        print(f"=== nó {no_id}: indisponível ({e.code().name})")
        return
    print(f"=== nó {r.no_id}: {r.papel} | termo {r.termo} | votou em {r.voto_concedido or '-'} | "
          f"líder {r.lider_id or '?'} | commit {r.indice_commit} | último índice {r.ultimo_indice}")
    for e in r.log:
        marca = "comitada " if e.indice <= r.indice_commit else "PENDENTE "
        op = f"{e.chave}={e.valor}" if e.operacao == "SET" else "NOOP"
        print(f"    [{e.indice:>3}] termo {e.termo}  {marca} {op}")
    print(f"    banco final: {dict(sorted(r.dados.items()))}")
    for rep in r.replicas:
        ms = f"{rep.ms_desde_ultimo_fetch:.0f} ms" if rep.ms_desde_ultimo_fetch >= 0 else "nunca"
        print(f"    réplica {rep.id}: {'ATIVA  ' if rep.ativa else 'INATIVA'} confirmou até "
              f"{rep.indice_confirmado} (último Fetch há {ms})")


def executar(argv):
    ap = argparse.ArgumentParser(prog="cliente.py", add_help=True)
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("gravar")
    g.add_argument("chave")
    g.add_argument("valor")
    g.add_argument("--no", type=int, choices=sorted(config.NOS))
    g.add_argument("--crash", action="store_true",
                   help="Cenário 4B: o líder grava no log e cai antes de replicar")
    lr = sub.add_parser("ler")
    lr.add_argument("chave", nargs="?")
    lr.add_argument("--no", type=int, choices=sorted(config.NOS), default=1)
    lt = sub.add_parser("ler-todos")
    lt.add_argument("chave", nargs="?")
    es = sub.add_parser("estado")
    es.add_argument("--no", type=int, choices=sorted(config.NOS))
    a = ap.parse_args(argv)

    if a.cmd == "gravar":
        gravar(a.chave, a.valor, a.no, a.crash)
    elif a.cmd == "ler":
        ler(a.chave, a.no)
    elif a.cmd == "ler-todos":
        for n in config.NOS:
            ler(a.chave, n)
    elif a.cmd == "estado":
        for n in ([a.no] if a.no else config.NOS):
            estado(n)


AJUDA = """Comandos:
  gravar <chave> <valor> [--no N] [--crash]   grava (redireciona ao líder se preciso)
  ler [<chave>] [--no N]                     lê dados comitados de um nó (padrão: nó 1)
  ler-todos [<chave>]                        lê de todos os nós
  estado [--no N]                            papel, termo, commit, log e réplicas
  sair"""


def interativo():
    print("Cliente do cluster de replicação (gRPC, modelo Pull)")
    print(AJUDA)
    while True:
        try:
            linha = input("\ncliente> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not linha:
            continue
        if linha in ("sair", "exit", "quit"):
            break
        if linha in ("ajuda", "help", "?"):
            print(AJUDA)
            continue
        try:
            executar(shlex.split(linha))
        except SystemExit:
            pass


if __name__ == "__main__":
    if len(sys.argv) > 1:
        executar(sys.argv[1:])
    else:
        interativo()
