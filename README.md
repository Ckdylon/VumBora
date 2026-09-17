# Vumbora - Sistema Distribuído de Caronas Compartilhadas

O **Vumbora** é uma aplicação cliente-servidor distribuída para gerenciamento, publicação e reserva de caronas intermunicipais compartilhadas, desenvolvida para o módulo de Concorrência e Conectividade (TEC502).

O projeto foi construído em Go e opera sobre sockets TCP/IP puros, utilizando um protocolo de aplicação baseado em mensagens JSON delimitadas por quebra de linha (`\n`). Todo o gerenciamento de estado compartilhado e concorrência no servidor segue estritamente o modelo **CSP (*Communicating Sequential Processes*)**, delegando o controle de dados a uma *Goroutine Monitora* e canais nativos, eliminando o uso de travas manuais (*mutexes*) e prevenindo condições de corrida por projeto.

---

## Estrutura do Projeto e Pacotes

A organização do repositório adota os padrões idiomáticos de Go (`cmd/` para pontos de entrada executáveis e `internal/` para pacotes de lógica de negócio e infraestrutura privada):

```text
vumbora/
├── cmd/
│   ├── servidor/             # Ponto de entrada do Servidor TCP central
│   │   └── main.go
│   ├── motorista/            # Interface de linha de comando (CLI) do Motorista
│   │   └── main.go
│   ├── passageiro/           # Interface de linha de comando (CLI) do Passageiro
│   │   └── main.go
│   └── teste_concorrencia/   # Script de teste de carga e race conditions (20 clientes)
│       └── main.go
├── internal/
│   ├── dominio/              # Modelos de dados de domínio (Carona, Trecho, Itinerario)
│   │   └── modelos.go
│   ├── roteamento/           # Gerenciador de estado CSP, rotas e atomicidade
│   │   ├── gerenciador.go
│   │   └── buscador.go
│   └── tcp/                  # Abstração de sockets TCP, dispatch de requests e JSON
│       ├── server.go
│       └── client.go
├── Dockerfile                # Build multi-binário da aplicação
├── docker-compose.yml        # Orquestração local em rede bridge
├── go.mod                    # Definição do módulo Go
└── README.md

```

### Descrição dos Pacotes

* **`cmd/*`**: Contém as funções `main()` independentes para cada nó do sistema distribuído.


* **`internal/dominio`**: Define as estruturas de dados centrais. Uma `Carona` é composta por metadados (motorista, data, vagas totais) e uma fatia contígua de `TrechoCarona` (origem, destino, ocupação e lista de passageiros).


* **`internal/roteamento`**: Núcleo transacional do servidor. O `GerenciadorCaronas` centraliza a memória e escuta requisições através de canais Go dentro de um laço `select`, garantindo serialização de escrita e reserva atômica de múltiplos trechos.


* **`internal/tcp`**: Camada de rede responsável por abrir sockets com `net.Listen` e `net.Dial`, gerenciar o ciclo de vida de conexões concorrentes e realizar o *unmarshaling* seguro dos payloads JSON.



---

## Protocolo de Aplicação (API TCP)

A comunicação ocorre via streaming TCP delimitado por `\n`. Todas as mensagens trafegam encapsuladas em envelopes estruturados:

* **Requisição do Cliente:**
```json
{"op": "NOMEOPERACAO", "data": { ...payload... }}

```


* **Resposta do Servidor:**
```json
{"ok": true, "msg": "mensagem descritiva", "res": { ...dadosopcionais... }}

```



| Operação (`op`)

 | Papel | Payload (`data`) |
| --- | --- | --- |
| `LOGIN`<br> | Autenticação inicial do socket

 | `{"email": string, "senha": string}` |
| `PUBLICAR`<br> | Publicação de carona pelo motorista

 | `{"motoristaid": string, "data": string, "preco": float, "assentos": int, "rota": [string]}` |
| `BUSCAR`<br> | Consulta de itinerários disponíveis

 | `{"origem": string, "destino": string, "data": string}` |
| `RESERVAR`<br> | Confirmação atômica de vaga

 | `{"caronaid": string, "origem": string, "destino": string, "passageiroid": string}` |
| `MINHASVIAGENS`<br> | Consulta de reservas do passageiro

 | `{"passageiroid": string}` |
| `MINHASCARONAS`<br> | Painel de controle do motorista

 | `{"motoristaid": string}` |
| `CANCELARRESERVA`<br> | Liberação de assento pelo passageiro

 | `{"caronaid": string, "origem": string, "destino": string, "passageiroid": string}` |
| `CANCELARTRECHO`<br> | Cancelamento de segmento pelo motorista

 | `{"caronaid": string, "motoristaid": string, "origem": string, "destino": string}` |

---

## Pré-requisitos

* **Go** versão 1.21 ou superior instalada (para execução nativa).
* **Docker** e **Docker Compose** (para execução em contêineres).
* Acesso à rede local (caso execute distribuído entre múltiplos computadores).

---

## Como Executar

### Opção 1: Execução Nativa com Go (Recomendada para Desenvolvimento)

**1. Iniciar o Servidor Central (Máquina 1):**

```bash
go run ./cmd/servidor

```

*O servidor iniciará escutando em `:8080` (todas as interfaces de rede).*

**2. Iniciar o Cliente Motorista (Máquina 2 ou mesmo PC):**

```bash
# Se estiver rodando na mesma máquina:
go run ./cmd/motorista

# Se estiver em outra máquina na mesma rede Wi-Fi:
SERVER_ADDR="192.168.1.X:8080" go run ./cmd/motorista

```

**3. Iniciar o Cliente Passageiro (Máquina 3 ou mesmo PC):**

```bash
# Se estiver rodando na mesma máquina:
go run ./cmd/passageiro

# Se estiver em outra máquina na mesma rede Wi-Fi:
SERVER_ADDR="192.168.1.X:8080" go run ./cmd/passageiro

```

---

### Opção 2: Execução via Docker Compose

**1. Subir o Servidor em segundo plano:**

```bash
docker compose up -d --build servidor

```

**2. Abrir o terminal interativo do Motorista:**

```bash
docker compose run --rm motorista

```

**3. Abrir o terminal interativo do Passageiro:**

```bash
docker compose run --rm passageiro

```

> **Nota para execução distribuída com Docker entre computadores distintos:**
> Ao rodar o cliente Docker em outro PC na mesma rede, repasse o IP do servidor via variável de ambiente:
> 
> 
> ```bash
> docker compose run --rm -e SERVER_ADDR="192.168.1.X:8080" passageiro
> 
> ```
> 
> 

---

## Guia de Uso

### Fluxo do Motorista

1. **Autenticação:** Informe seu e-mail e senha no prompt inicial.


2. **Publicar Carona (`[1]`):**
* Digite a data de partida (formato `DD-MM`, ex: `20-11`).
* Informe a quantidade total de assentos do veículo (ex: `3`).
* Informe o valor base por trecho (ex: `25.00`).
* Digite a rota sequencial separada por vírgulas (ex: `Feira de Santana, Salvador, Lauro de Freitas`). O servidor dividirá o trajeto em trechos individuais automaticamente.




3. **Minhas Caronas (`[2]`):** Acompanhe suas viagens ativas, visualizando os passageiros confirmados por segmento e o faturamento total acumulado.


4. **Cancelar Trecho (`[3]`):** Remove um segmento específico mantendo os demais ativos.



### Fluxo do Passageiro

1. **Autenticação:** Faça login com seu e-mail.


2. **Buscar e Reservar (`[1]`):**
* Informe a cidade de Origem e Destino desejada.


* Informe a data da viagem (`DD-MM`).


* O sistema listará os itinerários válidos somando o custo dos trechos. Escolha o índice da viagem para confirmar a reserva atômica.




3. **Minhas Viagens (`[2]`):** Visualize seus bilhetes confirmados.


4. **Cancelar Reserva (`[3]`):** Selecione uma de suas viagens confirmadas para liberar o assento instantaneamente no servidor.



---

## Teste Automatizado de Concorrência e Carga

Para validar a integridade transacional sob estresse e cumprir o **Item 10 do Barema**, o repositório inclui um executável que simula **20 conexões concorrentes disputando 1 única vaga restante no mesmo milissegundo**:

```bash
go run ./cmd/testeconcorrencia/main.go

```

### O que o teste avalia:

* **Barreira de largada:** As 20 conexões autenticam previamente e aguardam o sinal de disparo unificado para escrever nos sockets simultaneamente.


* **Atomicidade (Sem Overbooking):** Garante que **apenas 1 passageiro** receba confirmação e os outros 19 sejam rejeitados de forma limpa.


* **Desempenho:** Mede o tempo total do experimento e a latência média de resposta por socket (geralmente abaixo de 1 milissegundo sob o modelo CSP).



---

## Destaques de Engenharia e Arquitetura

* **Concorrência Segura (CSP):** Em conformidade com o princípio de Go *"Do not communicate by sharing memory; instead, share memory by communicating"*, as requisições de rede não acessam estruturas globais diretamente. Todas são serializadas via canais em uma única goroutine monitora, evitando condições de corrida (*race conditions*).


* **Atomicidade Transacional:** Ao reservar uma rota com múltiplos trechos, o sistema valida a disponibilidade de todos os segmentos antes de ocupar qualquer vaga. Se o último trecho estiver esgotado, a transação inteira é abortada, garantindo o princípio tudo-ou-nada sem bloqueios intermediários ou *deadlocks*.


* **Resiliência a Quedas de Rede:** Desconexões abruptas de clientes durante a navegação nos menus não afetam o estado das caronas já confirmadas na memória.
