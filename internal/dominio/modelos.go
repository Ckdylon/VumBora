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

//inicialmente so a criação das classes e depois a implementação das funções

//a ligação aqui é como? eu chamo o motorrista aqui ou eu chamo o veiculo la no motorista?
type Veiculo struct {
	//Motorista
	Placa           string `json:"placa"`
	Modelo          string `json:"modelo"`
	CapacidadeTotal int    `json:"capacidadetotal"`
}

//mesma duvida aterior em relação a carona, chama aqui ou lá
type Carona struct {
	MotoristaID         string         `json:"motoristaid"`
	Id                  string         `json:"id"`
	DataPartida         time.Time      `json:"datapartida"`
	AssentosDisponiveis int            `json:"assentosdisponiveis"`
	ValorPorTrecho      float64        `json:"valorportrecho"`
	Trechos             []TrechoCarona `json:"trechos"`
	Veiculo             Veiculo        `json:"veiculo"`
	//as funções são a parte
	//teste para ver se consegue apagar caronas sem apagar o hitorico
	Ativa bool `json:"ativa"`
}

//no go essas palavra com uma letra maiscula no meio tem alguma caracterista especifa ou é igual ou serve como palavra normal?
//TrechoCarona vai representar a viagem entre duas cidades
type TrechoCarona struct {
	CidadeOrigem     string   `json:"cidadeorigem"`
	CidadeDestino    string   `json:"cidadedestino"`
	AssentosOcupados int      `json:"assentosocupados"`
	Passageiros      []string `json:"passageiros"` //guarda o ID/email de quem reservou
	Cancelado        bool     `json:"cancelado"`   //cancelar apenas um trecho
}

//Intinerario vai ser a resposta do servidor para a busca de um passageiro
type Itinerario struct {
	CaronaID          string         `json:"caronaid"`
	TrechosConectados []TrechoCarona `json:"trechosconectados"`
	ValorTotal        float64        `json:"valortotal"`
}
