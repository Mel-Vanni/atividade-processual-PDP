import argparse
import paho.mqtt.client as mqtt
import threading, time, json, hashlib, random, concurrent.futures, socket
from tabulate import tabulate  

BROKER = "localhost"
PORT = 1883

INIT        = "sd/init"
ELECTION    = "sd/voting"
COORDINATOR = "sd/coordinator"
CHALLENGE   = "sd/challenge"
SOLUTION    = "sd/solution"
RESULT      = "sd/result"

INIT_RETRY = 1.0
ELECTION_RETRY = 1.0
MINING_WORKERS = 4
MINING_TIMEOUT = 30

class TransactionTable:
    def __init__(self):
        self.lock = threading.Lock()
        self.table = {}

    def new_tx(self, txid, challenge):
        with self.lock:
            self.table[int(txid)] = {
                "challenge": int(challenge),
                "solution": "",
                "winner": -1
            }


    def set_solution(self, txid, solution, clientID):
        with self.lock:
            txid = int(txid)
            if txid not in self.table or self.table[txid]["winner"] != -1:
                return False
            self.table[txid]["solution"] = solution
            self.table[txid]["winner"] = int(clientID)
            return True

    def get_challenge(self, txid):
        with self.lock:
            return self.table.get(int(txid), {}).get("challenge")

    def get_solution(self, txid):
        with self.lock:
            return self.table.get(int(txid), {}).get("solution")

    def get_winner(self, txid):
        with self.lock:
            return self.table.get(int(txid), {}).get("winner")

    def status(self, txid):
        with self.lock:
            txid = int(txid)
            if txid not in self.table:
                return None
            return "pendente" if self.table[txid]["winner"] == -1 else "resolvida"

    def is_pending(self, txid):
        with self.lock:
            txid = int(txid)
            return txid in self.table and self.table[txid]["winner"] == -1

    def all(self):
        with self.lock:
            return {k: dict(v) for k, v in self.table.items()}

def miner_worker(start_index, step, difficulty, stop_event, txid, challenge):
    i = start_index
    while not stop_event.is_set():
        s = f"sol{i}"
        data = f"{txid}:{challenge}:{s}".encode()
        h = bin(int(hashlib.sha1(data).hexdigest(), 16))[2:].zfill(160)
        if h.startswith("0" * difficulty):
            return s
        i += step
    return None

def mine_solution_parallel(difficulty, txid, challenge, workers=4, timeout=None):
    stop_event = threading.Event()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(miner_worker, w, workers, difficulty, stop_event, txid, challenge)
            for w in range(workers)
        ]
        done, _ = concurrent.futures.wait(futures, timeout=timeout, return_when=concurrent.futures.FIRST_COMPLETED)
        for f in done:
            try:
                res = f.result()
            except Exception:
                res = None
            if res:
                stop_event.set()
                return res
    return None

class SDNode:
    def __init__(self, n_participants, clientID=None, broker_host=None, init_timeout=10, election_timeout=10):
        self.n = max(int(n_participants), 5)
        self.clientID = int(clientID) if clientID is not None else random.randint(0, 65535)
        self.broker = broker_host if broker_host else BROKER

        self.mqtt = mqtt.Client(client_id=f"sd-node-{self.clientID}")
        self.mqtt.on_connect = self.on_connect
        self.mqtt.on_message = self.on_message

        self.init_set = set()
        self.init_event = threading.Event()
        self.init_timeout = init_timeout

        self.election_set = set()
        self.election_event = threading.Event()
        self.election_timeout = election_timeout

        self.is_controller = False
        self.tx_table = TransactionTable()

        self.active_challenge = None
        self.challenge_event = threading.Event()

        self.stop_all = threading.Event()
        self.current_txid = None

    def connect(self):
        try:
            socket.getaddrinfo(self.broker, PORT)
        except Exception as e:
            print(f"[{self.clientID}] ERRO: não foi possível resolver broker '{self.broker}': {e}")
            raise

        try:
            self.mqtt.connect(self.broker, PORT, keepalive=60)
        except Exception as e:
            print(f"[{self.clientID}] ERRO ao conectar no broker {self.broker}:{PORT} -> {e}")
            raise

        self.mqtt.loop_start()
        self.mqtt.subscribe(INIT)
        self.mqtt.subscribe(ELECTION)
        self.mqtt.subscribe(COORDINATOR)
        self.mqtt.subscribe(CHALLENGE)
        self.mqtt.subscribe(SOLUTION)
        self.mqtt.subscribe(RESULT)

    def on_connect(self, client, userdata, flags, rc):
        print(f"[{self.clientID}] Conectado ao broker (rc={rc})")

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
        except Exception:
            return
        topic = msg.topic

        if topic == INIT:
            cid = int(payload.get("clientID", -1))
            if cid not in self.init_set:
                self.init_set.add(cid)
                print(f"[{self.clientID}] Init viu {cid} (total {len(self.init_set)})")
            if len(self.init_set) >= self.n:
                self.init_event.set()

        elif topic == ELECTION:
            cid = int(payload.get("clientID", -1))
            vid = int(payload.get("voteID", 0))
            if (vid, cid) not in self.election_set:
                self.election_set.add((vid, cid))
                print(f"[{self.clientID}] Voto recebido {(vid, cid)} (total {len(self.election_set)})")
            if len(self.election_set) >= self.n:
                self.election_event.set()

        elif topic == COORDINATOR:
            leader = int(payload.get("clientID", -1))
            self.is_controller = (leader == self.clientID)
            print(f"[{self.clientID}] Líder anunciado: {leader}")

        elif topic == CHALLENGE:
            txid = int(payload.get("transactionID"))
            d = int(payload.get("challenge"))
            self.active_challenge = (txid, d)
            self.current_txid = txid
            self.tx_table.new_tx(txid, d)
            print(f"\n[{self.clientID}] Novo desafio recebido: tx={txid} d={d}")
            self.challenge_event.set()

        elif topic == SOLUTION and self.is_controller:
            cid = int(payload.get("clientID"))
            txid = int(payload.get("transactionID"))
            sol = payload.get("solution")
            self.handle_submission(cid, txid, sol)

        elif topic == RESULT:
            cid = int(payload.get("clientID"))
            txid = int(payload.get("transactionID"))
            r = int(payload.get("result"))
            sol = payload.get("solution")

            if txid in self.tx_table.table and r == 1:
                self.tx_table.table[txid]["winner"] = cid
                self.tx_table.table[txid]["solution"] = sol

            if cid == self.clientID:
                print(f"[{self.clientID}] Resultado da minha solução tx {txid}: "
                      f"{'ACEITA' if r != 0 else 'REJEITADA'}")

    def send_init_loop(self):
        msg = {"clientID": self.clientID}
        while not self.init_event.is_set() and not self.stop_all.is_set():
            self.mqtt.publish(INIT, json.dumps(msg), qos=1)
            time.sleep(INIT_RETRY)

    def send_election_loop(self, voteID):
        msg = {"clientID": self.clientID, "voteID": voteID}
        while not self.election_event.is_set() and not self.stop_all.is_set():
            self.mqtt.publish(ELECTION, json.dumps(msg), qos=1)
            time.sleep(ELECTION_RETRY)

    def perform_bully_election(self):
        my_vote = random.randint(0, 65535)
        self.election_set.add((my_vote, self.clientID))
        threading.Thread(target=self.send_election_loop, args=(my_vote,), daemon=True).start()

        start_time = time.time()
        while len(self.election_set) < self.n and not self.stop_all.is_set():
            if time.time() - start_time > self.election_timeout:
                print(f"[{self.clientID}] Timeout na eleição. Prosseguindo com votos recebidos.")
                break
            time.sleep(10)

        leader_vote, leader_cid = -1, -1
        for vote, cid in self.election_set:
            if vote > leader_vote or (vote == leader_vote and cid > leader_cid):
                leader_vote, leader_cid = vote, cid

        self.is_controller = (leader_cid == self.clientID)

        if self.is_controller:
            self.mqtt.publish(COORDINATOR, json.dumps({"clientID": leader_cid}), qos=1)

        print(f"[{self.clientID}] Eleição concluída. Líder = {leader_cid}")
        return leader_cid

    def controller_start(self):
        self.current_txid = 0
        difficulty = random.randint(1, 20)
        self.tx_table.new_tx(self.current_txid, difficulty)
        self.mqtt.publish(CHALLENGE, json.dumps({
            "transactionID": self.current_txid,
            "challenge": difficulty
        }), qos=1)
        print(f"[Controller {self.clientID}] Desafio inicial tx=0 d={difficulty}")
        self.print_table()

    def handle_submission(self, clientID, txid, solution):
        if not self.tx_table.is_pending(txid):
            self.mqtt.publish(RESULT, json.dumps({
                "clientID": clientID,
                "transactionID": txid,
                "solution": solution,
                "result": 0
            }), qos=1)
            return

        diff = self.tx_table.get_challenge(txid)

        data = f"{txid}:{diff}:{solution}".encode()
        h = bin(int(hashlib.sha1(data).hexdigest(), 16))[2:].zfill(160)

        if not h.startswith("0" * diff):
            self.mqtt.publish(RESULT, json.dumps({
                "clientID": clientID, "transactionID": txid,
                "solution": solution, "result": 0
            }), qos=1)
            return

        ok = self.tx_table.set_solution(txid, solution, clientID)
        if not ok:
            self.mqtt.publish(RESULT, json.dumps({
                "clientID": clientID, "transactionID": txid,
                "solution": solution, "result": 0
            }), qos=1)
            return

        print(f"[Controller {self.clientID}] tx={txid} resolvida por {clientID}")
        self.mqtt.publish(RESULT, json.dumps({
            "clientID": clientID,
            "transactionID": txid,
            "solution": solution,
            "result": 1
        }), qos=1)
        self.print_table()

        print(f"[Controller {self.clientID}] Aguardando 20s para próximo desafio…")
        for _ in range(20):
            if self.stop_all.is_set():
                return
            time.sleep(1)

        next_tx = txid + 1
        next_diff = random.randint(1, 20)
        self.tx_table.new_tx(next_tx, next_diff)
        self.mqtt.publish(CHALLENGE, json.dumps({
            "transactionID": next_tx,
            "challenge": next_diff
        }), qos=1)
        print(f"[Controller {self.clientID}] Novo desafio tx={next_tx} d={next_diff}")
        self.print_table()

    def print_table(self):
        data = []
        for tid, info in sorted(self.tx_table.all().items()):
            data.append([tid, info['challenge'], info['solution'], info['winner']])

        headers = ["TxID", "Challenge", "Solution", "Winner"]
        print("\n[Controller] Estado atual da tabela:")
        print(tabulate(data, headers=headers, tablefmt="grid"))
        print()

    def miner_loop(self):
        while not self.stop_all.is_set():
            self.challenge_event.wait()
            self.challenge_event.clear()

            if self.stop_all.is_set():
                break
            if not self.active_challenge:
                continue

            txid, d = self.active_challenge

            print(f"[{self.clientID}] Minerando tx={txid} d={d}…")

            sol = mine_solution_parallel(d, txid, d, workers=MINING_WORKERS, timeout=MINING_TIMEOUT)

            if sol:
                print(f"[{self.clientID}] Solução encontrada: {sol}")
                self.mqtt.publish(SOLUTION, json.dumps({
                    "clientID": self.clientID,
                    "transactionID": txid,
                    "solution": sol
                }), qos=1)
            else:
                print(f"[{self.clientID}] Timeout na mineração")

    def start(self):
        try:
            self.connect()
        except Exception:
            print(f"[{self.clientID}] Falha ao conectar ao broker '{self.broker}'.")
            return

        time.sleep(10)


        self.init_set.add(self.clientID)
        threading.Thread(target=self.send_init_loop, daemon=True).start()
        waited = self.init_event.wait(timeout=self.init_timeout)
        print(f"[{self.clientID}] Fase INIT concluída: {sorted(self.init_set)}")


        self.perform_bully_election()


        if self.is_controller:
            self.controller_start()
        else:

            threading.Thread(target=self.miner_loop, daemon=True).start()


        if self.is_controller:
            print(f"[{self.clientID}] Sou controlador — tabela será exibida aqui.")
        else:
            print(f"[{self.clientID}] Sou minerador — mineração automática ativada.")

        try:
            while not self.stop_all.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"\n[{self.clientID}] Interrompido por Ctrl+C. Encerrando…")
            self.stop_all.set()
        finally:
            self.mqtt.loop_stop()
            self.mqtt.disconnect()
            print(f"[{self.clientID}] Nó finalizado.")

def parse_args():
    p = argparse.ArgumentParser(description="SD Miner Node")
    p.add_argument("n", type=int, help="Número de participantes esperados")
    p.add_argument("--clientid", type=int, default=None)
    p.add_argument("--broker", type=str, default=BROKER)
    p.add_argument("--init-timeout", type=int, default=10)
    p.add_argument("--election-timeout", type=int, default=10)
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    node = SDNode(
        n_participants=args.n,
        clientID=args.clientid,
        broker_host=args.broker,
        init_timeout=args.init_timeout,
        election_timeout=args.election_timeout
    )
    node.start()