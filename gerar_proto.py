"""Gera as classes stub (replicacao_pb2.py e replicacao_pb2_grpc.py) a partir do .proto.

Equivale a:
    python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/replicacao.proto
É chamado automaticamente por no.py e cliente.py se os arquivos não existirem.
"""
import os
import sys

DIR = os.path.dirname(os.path.abspath(__file__))


def gerar():
    from grpc_tools import protoc
    import grpc_tools
    include = os.path.join(os.path.dirname(grpc_tools.__file__), "_proto")
    rc = protoc.main([
        "grpc_tools.protoc",
        f"-I{os.path.join(DIR, 'proto')}",
        f"-I{include}",
        f"--python_out={DIR}",
        f"--grpc_python_out={DIR}",
        os.path.join(DIR, "proto", "replicacao.proto"),
    ])
    if rc != 0:
        sys.exit("Falha ao compilar o .proto")


def garantir():
    if not os.path.exists(os.path.join(DIR, "replicacao_pb2.py")):
        gerar()


if __name__ == "__main__":
    gerar()
    print("Stubs gerados: replicacao_pb2.py e replicacao_pb2_grpc.py")
