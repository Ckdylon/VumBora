# Builder, usa a imagem oficial do Go para compilar
FROM golang:1.27-alpine AS builder 
#1.27 é a versao do lab
WORKDIR /app

# Copia todos os arquivos do projeto para dentro do contêiner
COPY . .

# Compila os servidor, motorista e passageiro
RUN go build -o /bin/servidor ./cmd/servidor
RUN go build -o /bin/motorista ./cmd/motorista
RUN go build -o /bin/passageiro ./cmd/passageiro

# Runner, sem o código fonte, apenas os executáveis
FROM alpine:latest

WORKDIR /app

# Traz os executáveis do estágio anterior
COPY --from=builder /bin/servidor .
COPY --from=builder /bin/motorista .
COPY --from=builder /bin/passageiro .

# Comando padrão
CMD ["./servidor"]