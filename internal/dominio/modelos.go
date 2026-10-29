package dominio

import "time"

// Criação de classes po bloco como esta no Diagrama, depois checar as ligações entre eles
// Parte das "classes"
// no Go a letra maiscula no inicio é para variaveis globais e a minuscula é privada
//codigo já com as strcuts tags, para trafegar na rede.

// Isso vai ser padrão para qualquer pessoa do sistema
type Usuario struct {
	Id    string `json:"id"`
	Nome  string `json:"nome"`
	Email string `json:"email"`
}

//armazenar os dados fisicos do carro associados a carona
type Veiculo struct {
	//Motorista
	Placa           string `json:"placa"`
	Modelo          string `json:"modelo"`
	CapacidadeTotal int    `json:"capacidadetotal"`
}

//prereserva é o bloqueio temporario do 2PC de um assento
//com tempo de expiracao (TTL) e estampa logica de Lamport para desempate.
type PreReserva struct {
	TxID         string    `json:"txid"`
	PassageiroID string    `json:"passageiroid"`
	EmpresaCoord string    `json:"empresacoord"`
	LamportTS    uint64    `json:"lamportts"`
	ExpiraEm     time.Time `json:"expiraem"`
}

//define a ordem total por requisitos
// menor contador logico de Lamport
// menor identificador lexicografico da empresa coordenadora
// menor identificador lexicografico da transacao (TxID)
func (p PreReserva) TemPrecedenciaSobre(outra PreReserva) bool {
	if p.LamportTS != outra.LamportTS {
		return p.LamportTS < outra.LamportTS
	}
	if p.EmpresaCoord != outra.EmpresaCoord {
		return p.EmpresaCoord < outra.EmpresaCoord
	}
	return p.TxID < outra.TxID
}

//representa uma oferta de viagem publicada no servidor de uma empresa
type Carona struct {
	EmpresaID           string         `json:"empresaid"`
	MotoristaID         string         `json:"motoristaid"`
	Id                  string         `json:"id"`
	DataPartida         time.Time      `json:"datapartida"`
	AssentosDisponiveis int            `json:"assentosdisponiveis"`
	ValorPorTrecho      float64        `json:"valorportrecho"`
	Trechos             []TrechoCarona `json:"trechos"`
	Veiculo             Veiculo        `json:"veiculo"`
	//teste para ver se consegue apagar caronas sem apagar o hitorico
	Ativa bool `json:"ativa"`
}

//no go essas palavra com uma letra maiscula no meio tem alguma caracterista especifa ou é igual ou serve como palavra normal?
//TrechoCarona vai representar a viagem entre duas cidades
type TrechoCarona struct {
	CidadeOrigem     string       `json:"cidadeorigem"`
	CidadeDestino    string       `json:"cidadedestino"`
	AssentosOcupados int          `json:"assentosocupados"`
	Passageiros      []string     `json:"passageiros"` //guarda o ID/email de quem reservou
	PreReservas      []PreReserva `json:"prereservas,omitempty"`
	Cancelado        bool         `json:"cancelado"` //cancelar apenas um trecho
}

//Intinerario vai ser a resposta do servidor para a busca de um passageiro
type Itinerario struct {
	CaronaID          string         `json:"caronaid"`
	TrechosConectados []TrechoCarona `json:"trechosconectados"`
	ValorTotal        float64        `json:"valortotal"`
}

//Parte do API REST (entre servidores)

//encapsula a resposta de listagem de caronas de uma empresa parceira.
type RestCaronasResponse struct {
	EmpresaID string   `json:"empresaid"`
	LamportTS uint64   `json:"lamportts"`
	Caronas   []Carona `json:"caronas"`
}

//representa o payload da parte de prepare/lock do 2PC
type RestPrepareRequest struct {
	TxID         string `json:"txid"`
	CaronaID     string `json:"caronaid"`
	Origem       string `json:"origem"`
	Destino      string `json:"destino"`
	PassageiroID string `json:"passageiroid"`
	EmpresaCoord string `json:"empresacoord"`
	LamportTS    uint64 `json:"lamportts"`
}

//representa o payload do commit ou abort do 2PC
type RestDecisionRequest struct {
	TxID         string `json:"txid"`
	CaronaID     string `json:"caronaid"`
	Origem       string `json:"origem"`
	Destino      string `json:"destino"`
	PassageiroID string `json:"passageiroid"`
	EmpresaCoord string `json:"empresacoord"`
	LamportTS    uint64 `json:"lamportts"`
}

//padroniza o retorno das chamadas de coordenacao 2PC via HTTP
type RestStatusResponse struct {
	Ok        bool   `json:"ok"`
	Msg       string `json:"msg"`
	EmpresaID string `json:"empresaid"`
	LamportTS uint64 `json:"lamportts"`
}
