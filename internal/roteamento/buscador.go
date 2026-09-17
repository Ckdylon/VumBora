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
	resp    chan []ItinerarioComposto
}

type reqReserva struct {
	caronaID     string
	origem       string
	destino      string
	passageiroID string
	resp         chan bool
}

type reqCancelarReserva struct {
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
	publicarChan         chan reqPublicar
	buscaChan            chan reqBusca
	reservaChan          chan reqReserva
	listarMotChan        chan reqListarMotorista
	listarPassChan       chan reqListarPassageiro
	cancelarCaronaChan   chan reqCancelarCarona
	cancelarTrechoChan   chan reqCancelarTrecho
	cancelarReservaChan  chan reqCancelarReserva
	reservarCompostaChan chan ReqReservaComposta
}

type TrechoViagem struct {
	CaronaID      string  `json:"caronaid"`
	MotoristaID   string  `json:"motoristaid"`
	CidadeOrigem  string  `json:"cidadeorigem"`
	CidadeDestino string  `json:"cidadedestino"`
	Valor         float64 `json:"valor"`
}

type ItinerarioComposto struct {
	ItinerarioID string         `json:"itinerarioid"`
	Trechos      []TrechoViagem `json:"trechos"`
	ValorTotal   float64        `json:"valortotal"`
	Baldeacoes   int            `json:"baldeacoes"`
}

type ItemReserva struct {
	CaronaID string
	Origem   string
	Destino  string
}

type ReqReservaComposta struct {
	PassageiroID string
	Itens        []ItemReserva
	Resp         chan bool
}

// Criação de um construtor
// o ponteiro * serve no Go para modificar a struct original em memoria
func NovoGerenciador() *GerenciadorCaronas {
	g := &GerenciadorCaronas{
		//inicializando os canais
		authChan:             make(chan reqAuth, 50),
		publicarChan:         make(chan reqPublicar, 50),
		buscaChan:            make(chan reqBusca, 50),
		reservaChan:          make(chan reqReserva, 100),
		listarMotChan:        make(chan reqListarMotorista, 50),
		listarPassChan:       make(chan reqListarPassageiro, 50),
		cancelarCaronaChan:   make(chan reqCancelarCarona, 50),
		cancelarTrechoChan:   make(chan reqCancelarTrecho, 50),
		cancelarReservaChan:  make(chan reqCancelarReserva, 50),
		reservarCompostaChan: make(chan ReqReservaComposta, 50),
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
			req.resp <- buscarComBaldeacao(caronas, req.origem, req.destino, req.data)

		case req := <-g.reservaChan:
			req.resp <- reservarMemoria(caronas, req.caronaID, req.origem, req.destino, req.passageiroID)

		case req := <-g.listarMotChan:
			req.resp <- listarMotoristaMemoria(caronas, req.motoristaID)

		case req := <-g.listarPassChan:
			req.resp <- listarPassageiroMemoria(caronas, req.passageiroID)

		case req := <-g.cancelarCaronaChan:
			req.resp <- cancelarCaronaMemoria(caronas, req.caronaID, req.motoristaID)

		case req := <-g.cancelarReservaChan:
			req.resp <- cancelarReservaMemoria(caronas, req.caronaID, req.origem, req.destino, req.passageiroID)
		case req := <-g.cancelarTrechoChan:
			req.resp <- cancelarTrechoMemoria(caronas, req.caronaID, req.motoristaID, req.origem, req.destino)
		case req := <-g.reservarCompostaChan:
			//Verificação atômica prévia (Checa TODOS os trechos de TODOS os motoristas)
			todosDisponiveis := true
			for _, item := range req.Itens {
				if !verificarDisponibilidade(caronas, item.CaronaID, item.Origem, item.Destino) {
					todosDisponiveis = false
					break
				}
			}

			// FASE 2: Confirmação ou Aborto imediato
			if !todosDisponiveis {
				req.Resp <- false // Se qualquer perna da viagem falhar, nenhuma vaga é consumida
				continue
			}

			// Efetiva a reserva em todas as caronas envolvidas
			for _, item := range req.Itens {
				efetivarReserva(caronas, item.CaronaID, item.Origem, item.Destino, req.PassageiroID)
			}
			req.Resp <- true
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

func (g *GerenciadorCaronas) BuscarItinerarios(origem, destino string, data time.Time) []ItinerarioComposto {
	resposta := make(chan []ItinerarioComposto)
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
func (g *GerenciadorCaronas) CancelarReserva(caronaID, origem, destino, passageiroID string) bool {
	resposta := make(chan bool)
	g.cancelarReservaChan <- reqCancelarReserva{
		caronaID:     caronaID,
		origem:       origem,
		destino:      destino,
		passageiroID: passageiroID,
		resp:         resposta,
	}
	return <-resposta
}

func cancelarReservaMemoria(caronas []dominio.Carona, caronaID, origem, destino, passageiroID string) bool {
	var caronaRef *dominio.Carona
	for i := range caronas {
		if caronas[i].Id == caronaID {
			caronaRef = &caronas[i]
			break
		}
	}

	if caronaRef == nil {
		return false
	}

	idxOrigem, idxDestino := -1, -1
	for i, tr := range caronaRef.Trechos {
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

	// Verifica se o passageiro realmente está em todos os trechos do percurso
	for i := idxOrigem; i <= idxDestino; i++ {
		encontrado := false
		for _, p := range caronaRef.Trechos[i].Passageiros {
			if p == passageiroID {
				encontrado = true
				break
			}
		}
		if !encontrado {
			return false
		}
	}

	// Remove o passageiro e libera o assento
	for i := idxOrigem; i <= idxDestino; i++ {
		trecho := &caronaRef.Trechos[i]
		if trecho.AssentosOcupados > 0 {
			trecho.AssentosOcupados--
		}
		var novaLista []string
		for _, p := range trecho.Passageiros {
			if p != passageiroID {
				novaLista = append(novaLista, p)
			}
		}
		trecho.Passageiros = novaLista
	}

	return true
}

// procura viagens diretas e viagens com 1 conexao entre motoristas
func buscarComBaldeacao(caronas []dominio.Carona, origem, destino string, data time.Time) []ItinerarioComposto {
	var resultados []ItinerarioComposto

	// 1. Caronas Diretas (Mesmo motorista)
	for _, c := range caronas {
		if c.DataPartida.Format("2006-01-02") != data.Format("2006-01-02") {
			continue
		}
		// Verifica se c possui o trecho origem -> destino com vagas...
		if temVagaDireta(c, origem, destino) {
			resultados = append(resultados, ItinerarioComposto{
				ItinerarioID: c.Id,
				Trechos: []TrechoViagem{
					{CaronaID: c.Id, MotoristaID: c.MotoristaID, CidadeOrigem: origem, CidadeDestino: destino, Valor: c.ValorPorTrecho},
				},
				ValorTotal: c.ValorPorTrecho,
				Baldeacoes: 0,
			})
		}
	}

	// 2. Caronas com Conexão (Motorista 1 faz Origem -> X, Motorista 2 faz X -> Destino)
	for _, c1 := range caronas {
		if c1.DataPartida.Format("2006-01-02") != data.Format("2006-01-02") {
			continue
		}

		for _, tr1 := range c1.Trechos {
			if tr1.CidadeOrigem != origem || tr1.AssentosOcupados >= c1.AssentosDisponiveis {
				continue
			}
			cidadeConexao := tr1.CidadeDestino

			// Procura outro motorista partindo da cidade de conexão
			for _, c2 := range caronas {
				if c2.Id == c1.Id || c2.DataPartida.Format("2006-01-02") != data.Format("2006-01-02") {
					continue
				}

				for _, tr2 := range c2.Trechos {
					if tr2.CidadeOrigem == cidadeConexao && tr2.CidadeDestino == destino && tr2.AssentosOcupados < c2.AssentosDisponiveis {
						resultados = append(resultados, ItinerarioComposto{
							ItinerarioID: fmt.Sprintf("%s+%s", c1.Id, c2.Id),
							Trechos: []TrechoViagem{
								{CaronaID: c1.Id, MotoristaID: c1.MotoristaID, CidadeOrigem: origem, CidadeDestino: cidadeConexao, Valor: c1.ValorPorTrecho},
								{CaronaID: c2.Id, MotoristaID: c2.MotoristaID, CidadeOrigem: cidadeConexao, CidadeDestino: destino, Valor: c2.ValorPorTrecho},
							},
							ValorTotal: c1.ValorPorTrecho + c2.ValorPorTrecho,
							Baldeacoes: 1,
						})
					}
				}
			}
		}
	}

	return resultados
}

func encontrarIndices(c dominio.Carona, origem, destino string) (int, int) {
	idxOrigem, idxDestino := -1, -1
	for i, tr := range c.Trechos {
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
	return idxOrigem, idxDestino
}

func trechosDisponiveis(c dominio.Carona, de, ate int) bool {
	for i := de; i <= ate; i++ {
		if c.Trechos[i].Cancelado || c.Trechos[i].AssentosOcupados >= c.AssentosDisponiveis {
			return false
		}
	}
	return true
}

// Verifica se há assentos livres em todos os trechos de uma carona
func temVagaDireta(c dominio.Carona, origem, destino string) bool {
	idxOrigem, idxDestino := -1, -1
	for i, tr := range c.Trechos {
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
		if c.Trechos[i].AssentosOcupados >= c.AssentosDisponiveis {
			return false
		}
	}
	return true
}

// Reutiliza temVagaDireta buscando a carona pelo ID
func verificarDisponibilidade(caronas []dominio.Carona, caronaID, origem, destino string) bool {
	for _, c := range caronas {
		if c.Id == caronaID {
			return temVagaDireta(c, origem, destino)
		}
	}
	return false
}

// Ocupa os assentos e registra o passageiro nos trechos selecionados
func efetivarReserva(caronas []dominio.Carona, caronaID, origem, destino, passageiroID string) {
	for i := range caronas {
		if caronas[i].Id == caronaID {
			idxOrigem, idxDestino := -1, -1
			for j, tr := range caronas[i].Trechos {
				if tr.CidadeOrigem == origem && idxOrigem == -1 {
					idxOrigem = j
				}
				if tr.CidadeDestino == destino && idxOrigem != -1 {
					idxDestino = j
					break
				}
			}

			if idxOrigem != -1 && idxDestino != -1 && idxOrigem <= idxDestino {
				for j := idxOrigem; j <= idxDestino; j++ {
					caronas[i].Trechos[j].AssentosOcupados++
					caronas[i].Trechos[j].Passageiros = append(caronas[i].Trechos[j].Passageiros, passageiroID)
				}
			}
			return
		}
	}
}
