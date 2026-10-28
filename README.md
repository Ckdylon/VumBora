# Vumbora - Sistema Distribuído de Caronas Compartilhadas (Consórcio Federado)

O **Vumbora** é uma aplicação distribuída e federada para gerenciamento, publicação, busca e reserva atômica de caronas intermunicipais compartilhadas, desenvolvida para o módulo de Concorrência e Conectividade (TEC502) da UEFS.

Na transição para o **Problema 2**, o projeto evoluiu de uma arquitetura centralizada para um **consórcio peer-to-peer entre empresas regionais autônomas** (`EMP_A`, `EMP_B`, `EMP_C`), operando sem servidor central, sem coordenador fixo e sem banco de dados compartilhado. Os clientes (motoristas e passageiros) conectam-se exclusivamente via sockets TCP/IP puros ao servidor da sua própria empresa (utilizando protocolo JSON delimitado por `\n`), enquanto os servidores cooperam entre si através de uma **API REST sobre HTTP/1.1**.

Todo o gerenciamento de estado compartilhado e concorrência local em cada servidor segue estritamente o modelo **CSP (*Communicating Sequential Processes*)**, delegando o controle de dados a uma *Goroutine Monitora* e canais nativos, eliminando o uso de travas manuais (*mutexes*). A coordenação distribuída entre empresas é garantida pelo protocolo **Two-Phase Commit (2PC)** com coordenador transiente, **Relógios Lógicos de Lamport** com preempção na Fase 1, bloqueios temporários com expiração (*lease* com TTL de 3s) e persistência atômica em disco.

---

## Estrutura do Projeto e Pacotes

A organização do repositório adota os padrões idiomáticos de Go (`cmd/` para pontos de entrada executáveis e `internal/` para pacotes de lógica de negócio e infraestrutura privada):

```text
vumbora/
├── cmd/
│   ├── servidor/             # Ponto de entrada do Servidor (TCP clientes + API REST consórcio)
│   │   └── main.go
│   ├── motorista/            # Interface de linha de comando (CLI) do Motorista
│   │   └── main.go
│   ├── passageiro/           # Interface de linha de comando (CLI) do Passageiro
│   │   └── main.go
│   ├── simulador/            # Simulador autônomo de carga e concorrência multi-empresa
│   │   └── main.go
│   └── testeconcorrencia/    # Script de teste de carga e race conditions local (20 clientes)
│       └── main.go
├── internal/
│   ├── dominio/              # Modelos de domínio (Carona, Trecho, PreReserva, DTOs REST)
│   │   └── modelos.go
│   ├── roteamento/           # Gerenciador CSP, Relógio de Lamport, 2PC, BFS e persistência
│   │   └── buscador.go
│   └── tcp/                  # Sockets TCP, dispatch JSON, endpoints REST e testes de falha
│       ├── servidor.go
│       └── consorcio_test.go
├── API_REST.md               # Especificação completa da API REST e Diagrama de Sequência
├── Dockerfile                # Build da imagem do servidor com volume de persistência
├── docker-compose.yml        # Orquestração do consórcio com 3 empresas isoladas
├── go.mod                    # Definição do módulo Go
└── README.md
```

### Descrição dos Pacotes

- **`cmd/*`**: Contém as funções `main()` independentes para cada nó do sistema distribuído (servidor da empresa, cliente motorista, cliente passageiro, simulador autônomo de carga e teste de concorrência).
- **`internal/dominio`**: Define as estruturas de dados centrais (`Carona`, `TrechoCarona`, `PreReserva`), a regra de precedência total do Relógio Lógico de Lamport (`TemPrecedenciaSobre`) e os contratos JSON da API REST.
- **`internal/roteamento`**: Núcleo transacional e motor de roteamento do servidor. O `GerenciadorCaronas` centraliza a memória via canais Go dentro de um laço `select` (`monitorCaronas`), gerencia a expiração de pré-reservas por TTL (`time.Ticker`), atua como coordenador transiente do protocolo 2PC, calcula rotas multi-salto entre $N$ empresas via Busca em Largura (BFS) e persiste o estado em disco.
- **`internal/tcp`**: Camada de rede responsável por escutar clientes via sockets TCP (`net.Listen`), expor os endpoints HTTP da API REST do consórcio (`net/http`) e executar a suíte de testes automatizados sob carga e injeção de falhas (`consorcio_test.go`).

---

## Protocolos de Aplicação (API TCP e API REST)

### 1. Protocolo Cliente-Servidor (Streaming TCP)

A comunicação entre os clientes CLI e o servidor da sua respectiva empresa ocorre via streaming TCP delimitado por `\n`:

- **Requisição do Cliente:**
```json
{"op": "NOMEOPERACAO", "data": { ...payload... }}
```

- **Resposta do Servidor:**
```json
{"ok": true, "msg": "mensagem descritiva", "res": { ...dadosopcionais... }}
```

| Operação (`op`) | Papel | Payload (`data`) |
| --- | --- | --- |
| `LOGIN` | Autenticação inicial do socket | `{"email": string, "senha": string}` |
| `PUBLICAR` | Publicação de carona pelo motorista | `{"motoristaid": string, "data": string, "preco": float, "assentos": int, "rota": [string]}` |
| `BUSCAR` | Busca federada de itinerários no consórcio | `{"origem": string, "destino": string, "data": string}` |
| `RESERVAR` | Confirmação atômica (Direta ou 2PC Composta) | `{"caronaid": string, "origem": string, "destino": string, "passageiroid": string}` |
| `MINHASVIAGENS` | Consulta federada de reservas do passageiro | `{"passageiroid": string}` |
| `MINHASCARONAS` | Painel de controle do motorista | `{"motoristaid": string}` |
| `CANCELARRESERVA` | Liberação de assento (local ou remota) | `{"caronaid": string, "origem": string, "destino": string, "passageiroid": string}` |
| `CANCELARTRECHO` | Cancelamento de segmento pelo motorista | `{"caronaid": string, "motoristaid": string, "origem": string, "destino": string}` |
| `CANCELAR` | Cancelamento integral da carona | `{"caronaid": string, "motoristaid": string}` |

### 2. Protocolo Inter-Servidores (API REST do Consórcio)

A cooperação entre os servidores das empresas utiliza HTTP/1.1 com payloads JSON e relógios lógicos de Lamport (`lamportts`). A documentação detalhada de payloads, códigos de retorno e o diagrama de sequência estão disponíveis em [`API_REST.md`](./API_REST.md):

| Método e Recurso | Papel no Consórcio | Códigos HTTP |
| --- | --- | --- |
| `GET /consorcio/caronas?data=YYYY-MM-DD` | Exporta caronas ativas da empresa para montagem do grafo BFS | `200 OK` |
| `POST /consorcio/reservas/preparar` | **Fase 1 do 2PC (`PREPARE`)**: Aloca pré-reserva com TTL de 3s e avalia precedência de Lamport | `200 OK`, `409 Conflict` |
| `POST /consorcio/reservas/confirmar` | **Fase 2 do 2PC (`COMMIT`)**: Efetiva a reserva definitivamente e grava em disco | `200 OK`, `409 Conflict` |
| `POST /consorcio/reservas/abortar` | **Fase 2 do 2PC (`ABORT`)**: Desfaz pré-reserva ou reverte commit em rollback compensatório | `200 OK` |
| `POST /consorcio/reservas/cancelar` | Cancela posteriormente uma reserva confirmada em empresa parceira | `200 OK`, `409 Conflict` |

---

## Pré-requisitos

- **Go** versão 1.21 ou superior instalada (para execução nativa, simulador e testes).
- **Docker** e **Docker Compose** (para execução isolada das 3 empresas em contêineres).
- Acesso à rede local (para execução distribuída entre múltiplos computadores no laboratório).

---

## Como Executar

### Opção 1: Execução do Consórcio via Docker Compose (Recomendada)

O arquivo `docker-compose.yml` sobe simultaneamente **3 servidores isolados** representando três empresas distintas do consórcio, cada uma com seu próprio volume persistente:
- **Empresa A (`EMP_A`)**: TCP em `localhost:8080` | REST em `localhost:8081`
- **Empresa B (`EMP_B`)**: TCP em `localhost:8090` | REST em `localhost:8091`
- **Empresa C (`EMP_C`)**: TCP em `localhost:8100` | REST em `localhost:8101`

**1. Subir os 3 servidores do consórcio em segundo plano:**

```bash
docker compose up --build -d
```

**2. Conectar o Cliente Motorista a uma das empresas (ex: Empresa A, B ou C):**

```bash
# Conectando à Empresa A (porta 8080):
go run ./cmd/motorista localhost:8080

# Conectando à Empresa B (porta 8090):
go run ./cmd/motorista localhost:8090

# Conectando à Empresa C (porta 8100):
go run ./cmd/motorista localhost:8100
```

**3. Conectar o Cliente Passageiro à sua empresa:**

```bash
# Passageiro conectado apenas à Empresa A busca e reserva trechos de A, B e C:
go run ./cmd/passageiro localhost:8080
```

> **Nota para execução em múltiplos computadores físicos no laboratório (LARSID / LADICA):**
> Em cada computador, suba o contêiner da respectiva empresa parametrizando a variável `PEERS` com os IPs reais das máquinas vizinhas:
> ```bash
> EMPRESA_ID=EMP_A TCP_ADDR=:8080 HTTP_ADDR=:8081 DATA_FILE=/data/emp_a.json PEERS="EMP_B=http://192.168.1.12:8081,EMP_C=http://192.168.1.13:8081" go run ./cmd/servidor
> ```

---

### Opção 2: Execução Nativa com Go (Em 3 Terminais Locais)

**Terminal 1 (Servidor Empresa A):**
```bash
EMPRESA_ID=EMP_A TCP_ADDR=:8080 HTTP_ADDR=:8081 DATA_FILE=emp_a.json PEERS="EMP_B=http://localhost:8091,EMP_C=http://localhost:8101" go run ./cmd/servidor
```

**Terminal 2 (Servidor Empresa B):**
```bash
EMPRESA_ID=EMP_B TCP_ADDR=:8090 HTTP_ADDR=:8091 DATA_FILE=emp_b.json PEERS="EMP_A=http://localhost:8081,EMP_C=http://localhost:8101" go run ./cmd/servidor
```

**Terminal 3 (Servidor Empresa C):**
```bash
EMPRESA_ID=EMP_C TCP_ADDR=:8100 HTTP_ADDR=:8101 DATA_FILE=emp_c.json PEERS="EMP_A=http://localhost:8081,EMP_B=http://localhost:8091" go run ./cmd/servidor
```

---

## Guia de Uso

### Fluxo do Motorista

1. **Autenticação:** Informe seu e-mail e senha no prompt inicial.
2. **Publicar Carona (`[1]`):**
   - Digite a data de partida (formato `DD-MM`, ex: `30-10`).
   - Informe a quantidade total de assentos do veículo (ex: `3`).
   - Informe o valor base por trecho (ex: `40.00`).
   - Digite a rota sequencial separada por vírgulas (ex: `Salvador, Feira`).
3. **Minhas Caronas (`[2]`):** Acompanhe suas viagens ativas e os passageiros confirmados por segmento.
4. **Cancelar Trecho (`[3]`):** Remove um segmento específico mantendo os demais ativos.

### Fluxo do Passageiro

1. **Autenticação:** Faça login no servidor da sua empresa com seu e-mail e senha.
2. **Buscar e Reservar (`[1]`):**
   - Informe a cidade de Origem (ex: `Salvador`) e Destino final (ex: `Montes Claros`).
   - Informe a data da viagem (`DD-MM`, ex: `30-10`).
   - O servidor consultará as empresas parceiras via REST, montará os itinerários compostos via BFS (exibindo `[Empresa - CaronaID]` de cada trecho) e somará o valor total.
   - Escolha o índice do itinerário para disparar a reserva atômica em duas fases (2PC) entre todas as empresas envolvidas.
3. **Minhas Viagens (`[2]`):** Visualize todos os seus trechos confirmados no consórcio.
4. **Cancelar Reserva (`[3]`):** Selecione uma viagem confirmada para liberar o assento imediatamente (local ou remotamente).

---

## Simulador de Carga e Testes Automatizados com Injeção de Falhas

### 1. Teste Automatizado sob Carga e Injeção de Falhas (`consorcio_test.go`)

Verifica de forma totalmente automatizada todos os invariantes distribuídos exigidos no Problema 2:

```bash
go test -v ./internal/tcp
```

**O que a suíte automatizada valida:**
- **Disputa concorrente multi-servidor pelo último assento:** 30 goroutines distribuídas simultaneamente entre os servidores `EMP_A`, `EMP_B` e `EMP_C` disputam itinerários compostos de 3 empresas com apenas 2 vagas disponíveis, atestando zero *overbooking* e desempate determinístico por Relógio de Lamport.
- **Injeção de queda de servidor no meio da reserva:** Interrompe o servidor `EMP_C` e verifica que tentativas de reserva da rota completa sofrem *rollback* imediato em `EMP_A` e `EMP_B`, sem deixar itinerários reservados pela metade.
- **Expiração de bloqueios órfãos por TTL e isolamento de falhas:** Verifica que pré-reservas de coordenadores que falharam expiram após 3 segundos liberando a vaga, e que rotas entre `EMP_A` e `EMP_B` continuam operando normalmente enquanto `EMP_C` está desligada.
- **Recuperação consistente pós-queda:** Reinicia o servidor `EMP_C` a partir do armazenamento em disco e comprova que o estado das reservas permanece íntegro em todo o consórcio.

### 2. Simulador Autônomo de Passageiros (`cmd/simulador`)

Gera autonomamente a malha inter-regional (`Salvador -> Feira -> Conquista -> Montes Claros`) nas 3 empresas e dispara uma rajada concorrente de 30 passageiros realizando buscas e reservas simultâneas via TCP:

```bash
# Com os 3 servidores rodando (via Docker Compose ou localmente):
go run ./cmd/simulador/main.go
```

---

## Destaques de Engenharia e Arquitetura

- **Concorrência Segura sem Locks (CSP):** As requisições vindas tanto dos sockets TCP quanto das rotas HTTP REST não acessam estruturas de memória diretamente; todas são serializadas via canais tipados na goroutine `monitorCaronas`.
- **Atomicidade Multi-Empresa (Protocolo 2PC sem Coordenador Fixo):** O servidor onde o passageiro está conectado atua como coordenador transiente, executando a Fase 1 (`PREPARE`) e a Fase 2 (`COMMIT` ou `ABORT`) em todas as empresas do itinerário.
- **Precedência Determinística (Relógios Lógicos de Lamport):** Na ausência de sincronização física de relógios, cada evento carrega a estampa lógica $(L_i, \text{EmpresaCoord}, \text{TxID})$. Em disputas simultâneas pelo último assento durante a janela de preparação, transações com menor estampa lógica preemptam bloqueios provisórios de menor prioridade.
- **Tolerância a Falhas com *Lease* (TTL) e Persistência Atômica:** Pré-reservas possuem tempo máximo de vida de 3 segundos para evitar retenção perpétua de assentos em caso de queda do coordenador, e todas as reservas confirmadas são gravadas atomicamente em disco (`write-and-rename`) para recuperação consistente após reinicializações.
