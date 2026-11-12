"""Nó do cluster: o mesmo código roda nos 4 nós.

Papéis: SEGUIDOR -> CANDIDATO -> LÍDER (eleição estilo Raft)
Replicação: modelo Pull (estilo KRaft/Kafka) - as réplicas buscam os dados
no líder com FetchRequest a cada 100 ms; o FetchRequest também é o heartbeat.

Uso:
    python no.py --id 1            (repita para 2, 3 e 4 em outros terminais)
    python no.py --id 1 --limpar   (apaga o estado persistido antes de iniciar)
"""
import argparse
import os
import random
import signal
import sys
import threading
import time
from concurrent import futures

import gerar_proto
gerar_proto.garantir()

import grpc  # noqa: E402
import replicacao_pb2 as pb  # noqa: E402
import replicacao_pb2_grpc as pb_grpc  # noqa: E402

import config  # noqa: E402
from persistencia import Persistencia  # noqa: E402

SEGUIDOR, CANDIDATO, LIDER = "SEGUIDOR", "CANDIDATO", "LIDER"


def agora():
    return time.monotonic()


def hora():
    return time.strftime("%H:%M:%S") + f".{int(time.time() * 1000) % 1000:03d}"


class No:
    def __init__(self, no_id, atraso_commit=0.0):
        self.id = no_id
        self.pares = [p for p in config.NOS if p != no_id]
        self.atraso_commit = atraso_commit      # só para demonstrar o Cenário 5
        self.lock = threading.RLock()
        self.cond_commit = threading.Condition(self.lock)
        self.rodando = True

        # ---------- estado persistente ----------
        self.disco = Persistencia(no_id)
        self.termo, self.voted_for = self.disco.carregar_estado()
        self.log = self.disco.carregar_log()               # log[i-1] é a entrada de índice i
        self.banco, self.ultimo_aplicado = self.disco.carregar_banco()

        # ---------- estado volátil ----------
        self.papel = SEGUIDOR
        self.lider_id = None
        self.indice_commit = self.ultimo_aplicado
        self.prazo_eleicao = self._novo_prazo_eleicao()
        self.proximo_descoberta = 0                         # rodízio p/ descobrir o líder

        # ---------- estado do líder ----------
        self.indice_confirmado = {}     # réplica -> último índice confirmado via Fetch
        self.ultimo_fetch = {}          # réplica -> instante do último FetchRequest
        self.replica_ativa = {}

        # stubs gRPC para os outros nós
        self.stubs = {
            p: pb_grpc.ServicoReplicacaoStub(grpc.insecure_channel(config.NOS[p]))
            for p in self.pares
        }

        self.msg(f"iniciado | termo={self.termo} voto={self.voted_for} "
                 f"log={len(self.log)} entradas | commit/aplicado={self.ultimo_aplicado} "
                 f"| dados={self.banco}")

    # ================================================================
    # utilitários
    # ================================================================
    def msg(self, texto):
        print(f"[{hora()}] [Nó {self.id} | {self.papel:9} | termo {self.termo} | "
              f"commit {self.indice_commit}] {texto}", flush=True)

    def _novo_prazo_eleicao(self):
        return agora() + random.uniform(config.ELEICAO_TIMEOUT_MIN, config.ELEICAO_TIMEOUT_MAX)

    def ultimo_indice(self):
        return len(self.log)

    def termo_em(self, indice):
        if indice <= 0 or indice > len(self.log):
            return 0
        return self.log[indice - 1]["termo"]

    def ultimo_termo(self):
        return self.termo_em(self.ultimo_indice())

    def _persistir_estado(self):
        self.disco.salvar_estado(self.termo, self.voted_for)

    def _persistir_log(self):
        self.disco.salvar_log(self.log)

    def _atualizar_termo(self, novo_termo, motivo):
        """Atualização obrigatória: viu termo maior -> adota e vira SEGUIDOR."""
        if novo_termo > self.termo:
            antigo, termo_antigo = self.papel, self.termo
            self.termo = novo_termo
            self.voted_for = None
            self.papel = SEGUIDOR
            self.lider_id = None
            self._persistir_estado()
            self.prazo_eleicao = self._novo_prazo_eleicao()
            self.cond_commit.notify_all()
            self.msg(f"termo maior recebido ({motivo}): termo {termo_antigo} -> {novo_termo}, "
                     f"papel {antigo} -> SEGUIDOR")

    def _aplicar_ate(self, novo_commit):
        """Efetiva no banco final as entradas com índice <= índice de commit."""
        if novo_commit <= self.indice_commit:
            return
        self.indice_commit = novo_commit
        alterou = False
        while self.ultimo_aplicado < self.indice_commit:
            self.ultimo_aplicado += 1
            e = self.log[self.ultimo_aplicado - 1]
            if e["operacao"] == "SET":
                self.banco[e["chave"]] = e["valor"]
                self.msg(f"COMMIT índice {e['indice']} (termo {e['termo']}): "
                         f"{e['chave']}={e['valor']} gravado no banco final")
            alterou = True
        if alterou:
            self.disco.salvar_banco(self.banco, self.ultimo_aplicado)
        self.cond_commit.notify_all()

    @staticmethod
    def _entrada_pb(e):
        return pb.EntradaLog(indice=e["indice"], termo=e["termo"], operacao=e["operacao"],
                             chave=e["chave"], valor=e["valor"])

    # ================================================================
    # RPCs do serviço de replicação (entre nós)
    # ================================================================
    def Fetch(self, req, context):
        with self.lock:
            if req.termo > self.termo:
                self._atualizar_termo(req.termo, f"Fetch do nó {req.replica_id}")

            if self.papel != LIDER:
                # Não sou o líder: indico quem eu acho que é.
                return pb.FetchResponse(termo=self.termo, sou_lider=False,
                                        lider_id=self.lider_id or 0)

            rid = req.replica_id
            self.ultimo_fetch[rid] = agora()
            if not self.replica_ativa.get(rid):
                self.replica_ativa[rid] = True
                self.msg(f"réplica {rid} ATIVA (Fetch recebido; último índice {req.ultimo_indice})")

            # ---- verificação de consistência do log da réplica ----
            fi, ft = req.ultimo_indice, req.ultimo_termo
            if fi > self.ultimo_indice() or (fi > 0 and self.termo_em(fi) != ft):
                # Divergência: devolve até onde a réplica deve manter o log.
                # Entradas do líder com termo > ultimo_termo da réplica certamente
                # não existem nela, então recuamos até um termo <= ft.
                ponto = min(fi - 1, self.ultimo_indice())
                while ponto > 0 and self.termo_em(ponto) > ft:
                    ponto -= 1
                self.msg(f"log da réplica {rid} DIVERGE (índice {fi}, termo {ft}) "
                         f"-> mandando truncar após índice {ponto}")
                return pb.FetchResponse(termo=self.termo, sou_lider=True, lider_id=self.id,
                                        divergente=True, truncar_ate=ponto,
                                        indice_commit=self.indice_commit)

            # ---- log consistente: registra a confirmação e tenta avançar o commit ----
            self.indice_confirmado[rid] = fi
            self._avancar_commit()

            novas = self.log[fi: fi + config.MAX_ENTRADAS_POR_FETCH]
            return pb.FetchResponse(termo=self.termo, sou_lider=True, lider_id=self.id,
                                    entradas=[self._entrada_pb(e) for e in novas],
                                    indice_commit=self.indice_commit)

    def _avancar_commit(self):
        """Commit = maior índice presente em pelo menos QUORUM nós (incluindo o líder),
        calculado a partir dos índices informados nos FetchRequests."""
        indices = sorted([self.ultimo_indice()] +
                         [self.indice_confirmado.get(p, 0) for p in self.pares], reverse=True)
        candidato = indices[config.QUORUM - 1]
        # Como no Raft, o líder só comita por contagem entradas do seu próprio termo
        # (as anteriores são comitadas junto, por consequência).
        if candidato > self.indice_commit and self.termo_em(candidato) == self.termo:
            self.msg(f"quórum confirmou até o índice {candidato} "
                     f"(confirmados: {dict(sorted(self.indice_confirmado.items()))})")
            self._aplicar_ate(candidato)

    def RequestVote(self, req, context):
        with self.lock:
            if req.termo > self.termo:
                self._atualizar_termo(req.termo, f"RequestVote do nó {req.candidato_id}")

            concedido = False
            motivo = ""
            if req.termo < self.termo:
                motivo = "termo do candidato é antigo"
            elif self.voted_for not in (None, req.candidato_id):
                motivo = f"já votei no nó {self.voted_for} neste termo"
            else:
                # Validação do log: o do candidato deve ser tão ou mais atualizado.
                meu_t, meu_i = self.ultimo_termo(), self.ultimo_indice()
                atualizado = (req.ultimo_termo > meu_t or
                              (req.ultimo_termo == meu_t and req.ultimo_indice >= meu_i))
                if not atualizado:
                    motivo = (f"log do candidato (termo {req.ultimo_termo}, índice "
                              f"{req.ultimo_indice}) menos atualizado que o meu "
                              f"(termo {meu_t}, índice {meu_i})")
                else:
                    concedido = True
                    self.voted_for = req.candidato_id
                    self._persistir_estado()          # persiste o voto antes de responder
                    self.lider_id = req.candidato_id  # provável líder: tenta ele primeiro
                    self.prazo_eleicao = self._novo_prazo_eleicao()

            if concedido:
                self.msg(f"VOTO concedido ao nó {req.candidato_id} no termo {req.termo}")
            else:
                self.msg(f"voto NEGADO ao nó {req.candidato_id} (termo {req.termo}): {motivo}")
            return pb.VoteResponse(termo=self.termo, voto_concedido=concedido)

    # ================================================================
    # RPCs do serviço do cliente
    # ================================================================
    def Gravar(self, req, context):
        with self.lock:
            if self.papel != LIDER:
                lid = self.lider_id or 0
                return pb.GravarResponse(
                    sucesso=False, lider_id=lid,
                    lider_endereco=config.NOS.get(lid, ""),
                    mensagem=f"nó {self.id} é {self.papel}; líder atual: "
                             f"{lid if lid else 'desconhecido'}")

            # Sem quórum, sem escrita: conta réplicas ativas (Fetch recente) + o líder.
            ativos = 1 + sum(1 for p in self.pares if self._esta_ativa(p))
            if ativos < config.QUORUM:
                self.msg(f"escrita {req.chave}={req.valor} RECUSADA: só {ativos} nós ativos "
                         f"(quórum = {config.QUORUM})")
                return pb.GravarResponse(
                    sucesso=False, lider_id=self.id, termo=self.termo,
                    mensagem=f"sem quórum: {ativos}/{config.N} nós ativos, "
                             f"necessário {config.QUORUM}. Escrita recusada.")

            # Grava no log local com o termo atual e o próximo índice sequencial.
            entrada = {"indice": self.ultimo_indice() + 1, "termo": self.termo,
                       "operacao": "SET", "chave": req.chave, "valor": req.valor}
            self.log.append(entrada)
            self._persistir_log()
            idx, termo = entrada["indice"], entrada["termo"]
            self.msg(f"escrita recebida: {req.chave}={req.valor} -> log índice {idx}, "
                     f"termo {termo} (ainda NÃO comitada)")

            if req.simular_crash:
                self.msg("SIMULANDO CRASH: entrada gravada só no log local; encerrando o processo agora!")
                os._exit(1)

            # Espera o índice de commit alcançar esta entrada.
            limite = agora() + config.TIMEOUT_COMMIT
            while (self.indice_commit < idx and self.papel == LIDER
                   and self.termo == termo and agora() < limite):
                self.cond_commit.wait(timeout=0.05)

            if self.indice_commit >= idx and self.termo_em(idx) == termo:
                return pb.GravarResponse(sucesso=True, indice=idx, termo=termo, lider_id=self.id,
                                         mensagem=f"comitado no índice {idx} (termo {termo})")
            return pb.GravarResponse(
                sucesso=False, indice=idx, termo=termo, lider_id=self.lider_id or 0,
                mensagem="não foi possível confirmar a replicação no quórum (não comitado)")

    def Ler(self, req, context):
        with self.lock:
            # Leitura eventual: só o banco final (dados comitados). Entradas apenas
            # no log (não comitadas) nunca aparecem aqui.
            base = dict(
                no_id=self.id, papel=self.papel, termo=self.termo,
                indice_commit=self.indice_commit, dados=dict(self.banco))
            if req.chave:
                if req.chave in self.banco:
                    return pb.LerResponse(encontrado=True, valor=self.banco[req.chave], **base)
                return pb.LerResponse(encontrado=False, **base)
            return pb.LerResponse(encontrado=True, **base)

    def Estado(self, req, context):
        with self.lock:
            reps = []
            if self.papel == LIDER:
                for p in self.pares:
                    ms = (agora() - self.ultimo_fetch[p]) * 1000 if self.ultimo_fetch.get(p) else -1
                    reps.append(pb.InfoReplica(id=p, indice_confirmado=self.indice_confirmado.get(p, 0),
                                               ativa=self._esta_ativa(p), ms_desde_ultimo_fetch=ms))
            return pb.EstadoResponse(
                no_id=self.id, papel=self.papel, termo=self.termo,
                voto_concedido=self.voted_for or 0, lider_id=self.lider_id or 0,
                indice_commit=self.indice_commit, ultimo_indice=self.ultimo_indice(),
                log=[self._entrada_pb(e) for e in self.log], dados=dict(self.banco),
                replicas=reps)

    # ================================================================
    # Líder
    # ================================================================
    def _esta_ativa(self, p):
        t = self.ultimo_fetch.get(p)
        return t is not None and agora() - t <= config.HEARTBEAT_TIMEOUT

    def _virar_lider(self):
        self.papel = LIDER
        self.lider_id = self.id
        self.indice_confirmado = {p: 0 for p in self.pares}
        self.ultimo_fetch = {}
        self.replica_ativa = {}
        # Entrada NOOP no novo termo: permite comitar as entradas de termos
        # anteriores assim que o quórum confirmar (regra do Raft).
        self.log.append({"indice": self.ultimo_indice() + 1, "termo": self.termo,
                         "operacao": "NOOP", "chave": "", "valor": ""})
        self._persistir_log()
        self.msg(f"*** ELEITO LÍDER do termo {self.termo} *** (NOOP no índice {self.ultimo_indice()})")

    def _loop_monitor_lider(self):
        """Temporizador do líder: marca réplicas inativas (Heartbeat Timeout individual)."""
        while self.rodando:
            time.sleep(0.05)
            with self.lock:
                if self.papel != LIDER:
                    continue
                for p in self.pares:
                    ativa = self._esta_ativa(p)
                    if self.replica_ativa.get(p) and not ativa:
                        self.replica_ativa[p] = False
                        ativos = 1 + sum(1 for q in self.pares if self._esta_ativa(q))
                        aviso = "" if ativos >= config.QUORUM else " -> SEM QUÓRUM, escritas bloqueadas"
                        self.msg(f"réplica {p} INATIVA (sem Fetch há mais de "
                                 f"{int(config.HEARTBEAT_TIMEOUT * 1000)} ms). "
                                 f"Ativos: {ativos}/{config.N}{aviso}")

    # ================================================================
    # Réplica: loop de Fetch ativo
    # ================================================================
    def _loop_fetch(self):
        while self.rodando:
            with self.lock:
                if self.papel == LIDER:
                    alvo = None
                elif self.lider_id and self.lider_id != self.id:
                    alvo = self.lider_id
                else:
                    # Líder desconhecido: pergunta a cada par (rodízio).
                    alvo = self.pares[self.proximo_descoberta % len(self.pares)]
                    self.proximo_descoberta += 1
                if alvo:
                    req = pb.FetchRequest(termo=self.termo, replica_id=self.id,
                                          ultimo_indice=self.ultimo_indice(),
                                          ultimo_termo=self.ultimo_termo())
            if alvo is None:
                time.sleep(config.INTERVALO_FETCH)
                continue

            try:
                resp = self.stubs[alvo].Fetch(req, timeout=config.TIMEOUT_RPC)
            except grpc.RpcError:
                time.sleep(config.INTERVALO_FETCH)   # líder fora do ar: o temporizador decide
                continue

            dormir = config.INTERVALO_FETCH
            with self.lock:
                if self.papel == LIDER:
                    continue
                if resp.termo > self.termo:
                    self._atualizar_termo(resp.termo, f"resposta de Fetch do nó {alvo}")
                if not resp.sou_lider:
                    dica = resp.lider_id
                    self.lider_id = dica if dica and dica not in (alvo, self.id) else None
                    pular = True
                elif resp.termo < self.termo:
                    pular = True                     # líder de termo antigo: ignora
                else:
                    pular = False
                if pular:
                    dormir = 0.02 if self.lider_id else config.INTERVALO_FETCH / 2
                else:
                    # Fetch bem-sucedido no líder atual
                    if self.papel == CANDIDATO:
                        self.msg(f"encontrado líder {alvo} do termo {resp.termo} -> vira SEGUIDOR")
                        self.papel = SEGUIDOR
                    if self.lider_id != alvo:
                        self.msg(f"seguindo o líder {alvo}")
                    self.lider_id = alvo
                    self.prazo_eleicao = self._novo_prazo_eleicao()   # reinicia o temporizador aleatório

                    if resp.divergente:
                        self._truncar(resp.truncar_ate)
                        continue                         # busca de novo imediatamente

                    # O índice de commit recebido só vale para entradas que eu JÁ tinha
                    # (e que confirmei neste Fetch). As entradas que chegam agora ficam
                    # no log, invisíveis para leitura, até o próximo FetchRequest.
                    ja_tinha = self.ultimo_indice()
                    self._aplicar_ate(min(resp.indice_commit, ja_tinha))

                    if resp.entradas:
                        for e in resp.entradas:
                            if e.indice != self.ultimo_indice() + 1:
                                break
                            self.log.append({"indice": e.indice, "termo": e.termo, "operacao": e.operacao,
                                             "chave": e.chave, "valor": e.valor})
                        self._persistir_log()
                        self.msg(f"Fetch trouxe {len(resp.entradas)} entrada(s) "
                                 f"(índices {resp.entradas[0].indice}..{resp.entradas[-1].indice}); "
                                 f"gravadas no log, NÃO comitadas ainda")
                        if self.atraso_commit > 0:
                            dormir = self.atraso_commit
                            self.prazo_eleicao = self._novo_prazo_eleicao() + dormir
                            self.msg(f"[demo] esperando {self.atraso_commit}s antes do próximo Fetch "
                                     f"(dados ficam invisíveis para leitura até lá)")
            time.sleep(dormir)

    def _truncar(self, manter_ate):
        if manter_ate < self.indice_commit:
            manter_ate = self.indice_commit         # nunca apaga entrada comitada
        removidas = self.log[manter_ate:]
        if not removidas:
            return
        self.log = self.log[:manter_ate]
        self._persistir_log()
        desc = ", ".join(f"[{e['indice']}|t{e['termo']}|{e['operacao']} {e['chave']}={e['valor']}]"
                         for e in removidas)
        self.msg(f"TRUNCANDO log: removidas {len(removidas)} entrada(s) não comitada(s): {desc}")

    # ================================================================
    # Eleição
    # ================================================================
    def _loop_eleicao(self):
        while self.rodando:
            time.sleep(0.02)
            with self.lock:
                deve = self.papel != LIDER and agora() >= self.prazo_eleicao
            if deve:
                self._eleicao()

    def _eleicao(self):
        with self.lock:
            self.termo += 1
            self.papel = CANDIDATO
            self.voted_for = self.id
            self.lider_id = None
            self._persistir_estado()
            self.prazo_eleicao = self._novo_prazo_eleicao()
            termo = self.termo
            req = pb.VoteRequest(termo=termo, candidato_id=self.id,
                                 ultimo_indice=self.ultimo_indice(), ultimo_termo=self.ultimo_termo())
            self.msg(f"temporizador expirou sem notícias do líder -> CANDIDATO no termo {termo}")

        votos = 1
        resultados = {}

        def pedir(p):
            try:
                resultados[p] = self.stubs[p].RequestVote(req, timeout=config.TIMEOUT_RPC)
            except grpc.RpcError:
                resultados[p] = None

        threads = [threading.Thread(target=pedir, args=(p,)) for p in self.pares]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with self.lock:
            if self.papel != CANDIDATO or self.termo != termo:
                return
            for p, r in resultados.items():
                if r is None:
                    continue
                if r.termo > self.termo:
                    self._atualizar_termo(r.termo, f"resposta de voto do nó {p}")
                    return
                if r.voto_concedido:
                    votos += 1
            self.msg(f"eleição do termo {termo}: {votos} voto(s) "
                     f"(precisa de {config.QUORUM}); respostas de "
                     f"{[p for p, r in resultados.items() if r is not None]}")
            if votos >= config.QUORUM:
                self._virar_lider()

    # ================================================================
    def _loop_status(self):
        while self.rodando:
            time.sleep(config.INTERVALO_STATUS)
            with self.lock:
                extra = ""
                if self.papel == LIDER:
                    ativos = [p for p in self.pares if self._esta_ativa(p)]
                    extra = f" | réplicas ativas {ativos}"
                elif self.lider_id:
                    extra = f" | líder {self.lider_id}"
                self.msg(f"STATUS último índice {self.ultimo_indice()}{extra} | dados {self.banco}")

    def iniciar(self):
        for alvo in (self._loop_fetch, self._loop_eleicao, self._loop_monitor_lider, self._loop_status):
            threading.Thread(target=alvo, daemon=True).start()


class ServicoReplicacao(pb_grpc.ServicoReplicacaoServicer):
    def __init__(self, no):
        self.no = no

    def Fetch(self, request, context):
        return self.no.Fetch(request, context)

    def RequestVote(self, request, context):
        return self.no.RequestVote(request, context)


class ServicoCliente(pb_grpc.ServicoClienteServicer):
    def __init__(self, no):
        self.no = no

    def Gravar(self, request, context):
        return self.no.Gravar(request, context)

    def Ler(self, request, context):
        return self.no.Ler(request, context)

    def Estado(self, request, context):
        return self.no.Estado(request, context)


def main():
    ap = argparse.ArgumentParser(description="Nó do cluster de replicação (modelo Pull)")
    ap.add_argument("--id", type=int, required=True, choices=sorted(config.NOS))
    ap.add_argument("--limpar", action="store_true", help="apaga o estado persistido deste nó")
    ap.add_argument("--atraso-commit", type=float, default=0.0,
                    help="[demo Cenário 5] segundos de espera antes do Fetch seguinte "
                         "quando chegam entradas novas")
    args = ap.parse_args()

    if args.limpar:
        Persistencia(args.id).limpar()

    no = No(args.id, atraso_commit=args.atraso_commit)
    servidor = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb_grpc.add_ServicoReplicacaoServicer_to_server(ServicoReplicacao(no), servidor)
    pb_grpc.add_ServicoClienteServicer_to_server(ServicoCliente(no), servidor)
    porta = config.NOS[args.id].split(":")[1]
    servidor.add_insecure_port(f"[::]:{porta}")
    servidor.start()
    no.msg(f"servidor gRPC escutando em {config.NOS[args.id]}")
    no.iniciar()
    signal.signal(signal.SIGINT, signal.default_int_handler)   # Ctrl+C derruba o nó
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        no.rodando = False
        servidor.stop(0)
        sys.exit(0)


if __name__ == "__main__":
    main()
