// Aqui vai armazenar as caronas na memoria
// Como daqui vai sair os dados e guardar passa uma ideia de roteador de wifi, logo nome roteamneto
// Canal é uma manipulação de goroutine
package roteamento

import (
	"fmt"
	"time"
	"vumbora/internal/dominio"
)

// cria o de um canal, onde lá na frente vai trasporta por onde vou obter a resposta
// tipos de mensagens do canal de autenticação
type reqAuth struct {
	email string
	senha string
	resp  chan bool
}

// tipos de mensagens do canal de caronas
type reqPublicar struct {
	carona dominio.Carona
	resp   chan string
}
type reqBusca struct {
	origem  string
	destino string
	data    time.Time
	resp    chan []dominio.Itinerario
}

type reqReserva struct {
	caronaID     string
	origem       string
	destino      string
	passageiroID string
	resp         chan bool
}

type reqListarMotorista struct {
	motoristaID string
	resp        chan []dominio.Carona
}

type reqListarPassageiro struct {
	passageiroID string
	resp         chan []MinhaViagem
}

type reqCancelarCarona struct {
	caronaID    string
	motoristaID string
	resp        chan bool
}

type reqCancelarTrecho struct {
	caronaID    string
	motoristaID string
	origem      string
	destino     string
	resp        chan bool
}

type MinhaViagem struct {
	CaronaID string  `json:"carona_id"`
	Origem   string  `json:"origem"`
	Destino  string  `json:"destino"`
	Data     string  `json:"data"`
	Valor    float64 `json:"valor"`
}

// o GerenciadorCaronas é o banco de dados em memoria
type GerenciadorCaronas struct {
	//canal para autenticação
	authChan chan reqAuth

	//canais dedicados para gestão de viagens
	//usando um para cada funcionalidade
	publicarChan       chan reqPublicar
	buscaChan          chan reqBusca
	reservaChan        chan reqReserva
	listarMotChan      chan reqListarMotorista
	listarPassChan     chan reqListarPassageiro
	cancelarCaronaChan chan reqCancelarCarona
	cancelarTrechoChan chan reqCancelarTrecho
}

// Criação de um construtor
// o ponteiro * serve no Go para modificar a struct original em memoria
func NovoGerenciador() *GerenciadorCaronas {
	g := &GerenciadorCaronas{
		authChan:           make(chan reqAuth, 50),
		publicarChan:       make(chan reqPublicar, 50),
		buscaChan:          make(chan reqBusca, 50),
		reservaChan:        make(chan reqReserva, 100),
		listarMotChan:      make(chan reqListarMotorista, 50),
		listarPassChan:     make(chan reqListarPassageiro, 50),
		cancelarCaronaChan: make(chan reqCancelarCarona, 50),
		cancelarTrechoChan: make(chan reqCancelarTrecho, 50),
		//inicialização do canal principal. o 100 ou 50 é uma recomendação da documentação do Go, para o buffer não travar imediatemente
	}
	// obter um contexto no momento em quq o sistema sobe
	// sendo dois monitores concorrentes mas independetes
	go g.monitorAutenticacao()
	go g.monitorCaronas()
	return g
}

// autenticação do usuario, isolado
func (g *GerenciadorCaronas) monitorAutenticacao() {
	//no Go tem o padrão "comma ok" para buscar em mapas
	usuarios := make(map[string]string)
	for req := range g.authChan {
		senhaSalva, existe := usuarios[req.email]
		if !existe {
			//auto cadastro
			usuarios[req.email] = req.senha
			req.resp <- true
			continue
		}
		//se existe, verifica se a senha bate
		req.resp <- (senhaSalva == req.senha)
	}
}

// monitor de caronas isolado, lista de caronas separada de contador de ID
func (g *GerenciadorCaronas) monitorCaronas() {
	caronas := make([]dominio.Carona, 0)
	contadorID := 0

	for {
		select {
		case req := <-g.publicarChan:
			contadorID++
			req.carona.Id = fmt.Sprintf("C%d", contadorID)
			req.carona.Ativa = true
			caronas = append(caronas, req.carona)
			req.resp <- req.carona.Id

		case req := <-g.buscaChan:
			req.resp <- buscarMemoria(caronas, req.origem, req.destino, req.data)

		case req := <-g.reservaChan:
			req.resp <- reservarMemoria(caronas, req.caronaID, req.origem, req.destino, req.passageiroID)

		case req := <-g.listarMotChan:
			req.resp <- listarMotoristaMemoria(caronas, req.motoristaID)

		case req := <-g.listarPassChan:
			req.resp <- listarPassageiroMemoria(caronas, req.passageiroID)

		case req := <-g.cancelarCaronaChan:
			req.resp <- cancelarCaronaMemoria(caronas, req.caronaID, req.motoristaID)

		case req := <-g.cancelarTrechoChan:
			req.resp <- cancelarTrechoMemoria(caronas, req.caronaID, req.motoristaID, req.origem, req.destino)
		}
	}
}

// um API publica
// a goroutine atual fica bloqueada esperandoa resposta da principal
func (g *GerenciadorCaronas) Autenticar(email, senha string) bool {
	resposta := make(chan bool)
	g.authChan <- reqAuth{email: email, senha: senha, resp: resposta}
	return <-resposta
}

func (g *GerenciadorCaronas) PublicarCarona(c dominio.Carona) string {
	resposta := make(chan string)
	g.publicarChan <- reqPublicar{carona: c, resp: resposta}
	return <-resposta
}

func (g *GerenciadorCaronas) BuscarItinerarios(origem, destino string, data time.Time) []dominio.Itinerario {
	resposta := make(chan []dominio.Itinerario)
	g.buscaChan <- reqBusca{origem: origem, destino: destino, data: data, resp: resposta}
	return <-resposta
}

func (g *GerenciadorCaronas) Reservar(caronaID, origem, destino, passageiroID string) bool {
	resposta := make(chan bool)
	g.reservaChan <- reqReserva{
		caronaID:     caronaID,
		origem:       origem,
		destino:      destino,
		passageiroID: passageiroID,
		resp:         resposta,
	}
	return <-resposta
}

func (g *GerenciadorCaronas) ListarPorMotorista(motoristaID string) []dominio.Carona {
	resposta := make(chan []dominio.Carona)
	g.listarMotChan <- reqListarMotorista{motoristaID: motoristaID, resp: resposta}
	return <-resposta
}

func (g *GerenciadorCaronas) ListarPorPassageiro(passageiroID string) []MinhaViagem {
	resposta := make(chan []MinhaViagem)
	g.listarPassChan <- reqListarPassageiro{passageiroID: passageiroID, resp: resposta}
	return <-resposta
}

func (g *GerenciadorCaronas) CancelarCarona(caronaID, motoristaID string) bool {
	resposta := make(chan bool)
	g.cancelarCaronaChan <- reqCancelarCarona{caronaID: caronaID, motoristaID: motoristaID, resp: resposta}
	return <-resposta
}

func (g *GerenciadorCaronas) CancelarTrecho(caronaID, motoristaID, origem, destino string) bool {
	resposta := make(chan bool)
	g.cancelarTrechoChan <- reqCancelarTrecho{
		caronaID:    caronaID,
		motoristaID: motoristaID,
		origem:      origem,
		destino:     destino,
		resp:        resposta,
	}
	return <-resposta
}

func buscarMemoria(caronas []dominio.Carona, origem, destino string, data time.Time) []dominio.Itinerario {
	//encotrar a carona
	//esse _ é para nao precisar declara uma variavel, para mim não importa a posição da carona. Mas, se nao fizer isso o Go nao compila
	var resultados []dominio.Itinerario
	for _, carona := range caronas {
		if !carona.Ativa {
			continue
		}
		//trabalhando so com a data primeiro, para comparar
		if carona.DataPartida.Format("2006-01-02") != data.Format("2006-01-02") {
			continue
		}
		//indetificar o indice dos trechos e se todos tem vaga
		idxOrigem, idxDestino := -1, -1
		for i, tr := range carona.Trechos {
			if tr.Cancelado {
				continue
			}
			if tr.CidadeOrigem == origem && idxOrigem == -1 {
				idxOrigem = i
			}
			if tr.CidadeDestino == destino && idxOrigem != -1 {
				idxDestino = i
				break
			}
		}
		//se a rota for invalida, prenveção
		if idxOrigem != -1 && idxDestino != -1 && idxOrigem <= idxDestino {
			valido := true
			var trechosValidos []dominio.TrechoCarona
			for i := idxOrigem; i <= idxDestino; i++ {
				if carona.Trechos[i].Cancelado || carona.Trechos[i].AssentosOcupados >= carona.AssentosDisponiveis {
					valido = false
					break
				}
				trechosValidos = append(trechosValidos, carona.Trechos[i])
			}

			if valido {
				resultados = append(resultados, dominio.Itinerario{
					CaronaID:          carona.Id,
					TrechosConectados: trechosValidos,
					ValorTotal:        float64(len(trechosValidos)) * carona.ValorPorTrecho,
				})
			}
		}
	}
	return resultados
}

func reservarMemoria(caronas []dominio.Carona, caronaID, origem, destino, passageiroID string) bool {
	var caronaRef *dominio.Carona
	for i := range caronas {
		if caronas[i].Id == caronaID {
			caronaRef = &caronas[i]
			break
		}
	}

	if caronaRef == nil || !caronaRef.Ativa {
		return false
	}

	idxOrigem, idxDestino := -1, -1
	for i, tr := range caronaRef.Trechos {
		if tr.Cancelado {
			continue
		}
		if tr.CidadeOrigem == origem && idxOrigem == -1 {
			idxOrigem = i
		}
		if tr.CidadeDestino == destino && idxOrigem != -1 {
			idxDestino = i
			break
		}
	}

	if idxOrigem == -1 || idxDestino == -1 || idxOrigem > idxDestino {
		return false
	}

	for i := idxOrigem; i <= idxDestino; i++ {
		if caronaRef.Trechos[i].Cancelado || caronaRef.Trechos[i].AssentosOcupados >= caronaRef.AssentosDisponiveis {
			return false
		}
	}

	for i := idxOrigem; i <= idxDestino; i++ {
		caronaRef.Trechos[i].AssentosOcupados++
		caronaRef.Trechos[i].Passageiros = append(caronaRef.Trechos[i].Passageiros, passageiroID)
	}
	return true
}

func listarMotoristaMemoria(caronas []dominio.Carona, motoristaID string) []dominio.Carona {
	var lista []dominio.Carona
	for _, c := range caronas {
		if c.MotoristaID == motoristaID && c.Ativa {
			lista = append(lista, c)
		}
	}
	return lista
}

func listarPassageiroMemoria(caronas []dominio.Carona, passageiroID string) []MinhaViagem {
	var viagens []MinhaViagem
	for _, c := range caronas {
		if !c.Ativa {
			continue
		}
		for _, tr := range c.Trechos {
			if tr.Cancelado {
				continue
			}
			for _, p := range tr.Passageiros {
				if p == passageiroID {
					viagens = append(viagens, MinhaViagem{
						CaronaID: c.Id,
						Origem:   tr.CidadeOrigem,
						Destino:  tr.CidadeDestino,
						Data:     c.DataPartida.Format("02/01/2006"),
						Valor:    c.ValorPorTrecho,
					})
				}
			}
		}
	}
	return viagens
}

func cancelarCaronaMemoria(caronas []dominio.Carona, caronaID, motoristaID string) bool {
	for i := range caronas {
		if caronas[i].Id == caronaID && caronas[i].MotoristaID == motoristaID {
			caronas[i].Ativa = false
			return true
		}
	}
	return false
}

func cancelarTrechoMemoria(caronas []dominio.Carona, caronaID, motoristaID, origem, destino string) bool {
	for i := range caronas {
		if caronas[i].Id == caronaID && caronas[i].MotoristaID == motoristaID {
			for j := range caronas[i].Trechos {
				tr := &caronas[i].Trechos[j]
				if tr.CidadeOrigem == origem && tr.CidadeDestino == destino {
					tr.Cancelado = true
					return true
				}
			}
		}
	}
	return false
}
