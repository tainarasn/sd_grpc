"""Persistência em disco do estado de um nó.

Cada nó grava em dados/no<ID>/:
  estado.json -> termo atual e voto concedido (voted_for)
  log.json    -> log de operações (entradas com índice e termo)
  banco.json  -> banco final (somente dados comitados) + último índice aplicado

A escrita é atômica (arquivo temporário + fsync + os.replace), então um
desligamento no meio da gravação não corrompe o arquivo.
"""
import json
import os
import shutil


class Persistencia:
    def __init__(self, no_id, base="dados"):
        self.dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), base, f"no{no_id}")
        os.makedirs(self.dir, exist_ok=True)

    def limpar(self):
        shutil.rmtree(self.dir, ignore_errors=True)
        os.makedirs(self.dir, exist_ok=True)

    # ---------------- utilitários ----------------
    def _caminho(self, nome):
        return os.path.join(self.dir, nome)

    def _gravar(self, nome, obj):
        tmp = self._caminho(nome + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=1)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._caminho(nome))

    def _ler(self, nome, padrao):
        try:
            with open(self._caminho(nome), encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return padrao

    # ---------------- termo e voto ----------------
    def salvar_estado(self, termo, voto):
        self._gravar("estado.json", {"termo": termo, "voted_for": voto})

    def carregar_estado(self):
        d = self._ler("estado.json", {"termo": 0, "voted_for": None})
        return d["termo"], d["voted_for"]

    # ---------------- log de operações ----------------
    def salvar_log(self, log):
        self._gravar("log.json", log)

    def carregar_log(self):
        return self._ler("log.json", [])

    # ---------------- banco final (dados comitados) ----------------
    def salvar_banco(self, dados, ultimo_aplicado):
        self._gravar("banco.json", {"ultimo_aplicado": ultimo_aplicado, "dados": dados})

    def carregar_banco(self):
        d = self._ler("banco.json", {"ultimo_aplicado": 0, "dados": {}})
        return d["dados"], d["ultimo_aplicado"]
