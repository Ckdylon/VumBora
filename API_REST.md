# Especificação da API REST do Consórcio Vumbora (VaiJunto)

## 1. Visão Geral
Os servidores das empresas integrantes do consórcio comunicam-se exclusivamente via protocolo HTTP/1.1 sob arquitetura REST com payloads codificados em JSON (`Content-Type: application/json`). Não existe servidor central nem armazenamento compartilhado.

Para sincronização causal de eventos sem dependência de relógios físicos (NTP), todas as requisições e respostas trafegam o contador lógico de Lamport no campo `lamportts`. Ao receber uma mensagem com estampa $L_{\text{remoto}}$, o servidor receptor atualiza seu relógio local segundo a regra:
$$L_{\text{local}} \leftarrow \max(L_{\text{local}}, L_{\text{remoto}}) + 1$$

---

## 2. Recursos e Endpoints

### 2.1. Consulta de Caronas Disponíveis na Empresa Parceira
Retorna as caronas ativas gerenciadas localmente pela empresa consultada para composição de itinerários federados.

- **Método e Caminho:** `GET /consorcio/caronas?data=YYYY-MM-DD`
- **Parâmetros de Query (Opcional):**
  - `data`: Filtra as caronas ativas pela data de partida no formato `2006-01-02`.
- **Respostas:**
  - `200 OK`: Retorna a lista de caronas ativas e o relógio lógico atual do servidor.
```json
{
  "empresaid": "EMP_B",
  "lamportts": 14,
  "caronas": [
    {
      "empresaid": "EMP_B",
      "motoristaid": "mot_b@email.com",
      "id": "EMP_B-C1",
      "datapartida": "2026-10-30T00:00:00Z",
      "assentosdisponiveis": 2,
      "valorportrecho": 45.0,
      "trechos": [
        {
          "cidadeorigem": "Feira",
          "cidadedestino": "Conquista",
          "assentosocupados": 1,
          "passageiros": ["pass1@email.com"],
          "cancelado": false
        }
      ],
      "ativa": true
    }
  ]
}
