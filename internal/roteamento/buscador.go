// Aqui vai armazenar as caronas na memoria
// Como daqui vai sair os dados e guardar passa uma ideia de roteador de wifi, logo nome roteamneto
// Canal é uma manipulação de goroutine
package roteamento

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
	"vumbora/internal/dominio"
)

const ttlPreReserva = 3 * time.Second

//CANAIS CSP

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
type reqCaronasLocais struct {
	data       string
	lamportRem uint64
	resp       chan dominio.RestCaronasResponse
}

type reqPreReserva struct {
	dados dominio.RestPrepareRequest
	resp  chan dominio.RestStatusResponse
}

type reqDecisaoReserva struct {
	dados dominio.RestDecisionRequest
	resp  chan dominio.RestStatusResponse
}

type reqClock struct {
	lamportRem uint64
	resp       chan uint64
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

type reqCancelarReserva struct {
	caronaID     string
	origem       string
	destino      string
	passageiroID string
	resp         chan bool
}

//retorno e rotemanto

type MinhaViagem struct {
	CaronaID string  `json:"caronaid"`
	Origem   string  `json:"origem"`
	Destino  string  `json:"destino"`
	Data     string  `json:"data"`
	Valor    float64 `json:"valor"`
}

type TrechoViagem struct {
	EmpresaID     string  `json:"empresaid"`
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
	EmpresaID string
	CaronaID  string
	Origem    string
	Destino   string
}

type ReqReservaComposta struct {
	PassageiroID string
	Itens        []ItemReserva
	Resp         chan bool
}

// armazena os dados da empresa em disco para recuperacao pos-falha
type estadoPersistido struct {
	EmpresaID  string           `json:"empresaid"`
	ContadorID int              `json:"contadorid"`
	LamportTS  uint64           `json:"lamportts"`
	Caronas    []dominio.Carona `json:"caronas"`
}

// guarda informacoes para compensar se falhar em outro
type registroCommit struct {
	CaronaID     string
	IdxOrigem    int
	IdxDestino   int
	PassageiroID string
}

// o GerenciadorCaronas é o banco de dados que centraliza os canais CSP e a configuracao do no no consorcio
type GerenciadorCaronas struct {
	//canal para autenticação
	authChan chan reqAuth

	empresaID     string
	arquivoEstado string
	peers         map[string]string // map[EmpresaID]URL_HTTP
	httpClient    *http.Client

	//canais dedicados para gestão de viagens
	//usando um para cada funcionalidade
	publicarChan        chan reqPublicar
	caronasLocaisChan   chan reqCaronasLocais
	preReservaChan      chan reqPreReserva
	confirmarChan       chan reqDecisaoReserva
	abortarChan         chan reqDecisaoReserva
	clockChan           chan reqClock
	listarMotChan       chan reqListarMotorista
	listarPassChan      chan reqListarPassageiro
	cancelarCaronaChan  chan reqCancelarCarona
	cancelarTrechoChan  chan reqCancelarTrecho
	cancelarReservaChan chan reqCancelarReserva
}

// Criação de um construtor
// o ponteiro * serve no Go para modificar a struct original em memoria
func NovoGerenciador() *GerenciadorCaronas {
	empID := os.Getenv("EMPRESA_ID")
	if empID == "" {
		empID = "EMP_A"
	}
	arq := os.Getenv("DATA_FILE")
	peers := parsePeersEnv(os.Getenv("PEERS"))
	return NovoGerenciadorConsorcio(empID, arq, peers)
}

// permite instanciar multiplas empresas
func NovoGerenciadorConsorcio(empresaID, arquivoEstado string, peers map[string]string) *GerenciadorCaronas {
	if peers == nil {
		peers = make(map[string]string)
	}
	//inicializando os canais
	g := &GerenciadorCaronas{
		empresaID:           empresaID,
		arquivoEstado:       arquivoEstado,
		peers:               peers,
		httpClient:          &http.Client{Timeout: 1200 * time.Millisecond},
		authChan:            make(chan reqAuth, 100),
		publicarChan:        make(chan reqPublicar, 100),
		caronasLocaisChan:   make(chan reqCaronasLocais, 100),
		preReservaChan:      make(chan reqPreReserva, 200),
		confirmarChan:       make(chan reqDecisaoReserva, 200),
		abortarChan:         make(chan reqDecisaoReserva, 200),
		clockChan:           make(chan reqClock, 200),
		listarMotChan:       make(chan reqListarMotorista, 100),
		listarPassChan:      make(chan reqListarPassageiro, 100),
		cancelarCaronaChan:  make(chan reqCancelarCarona, 100),
		cancelarTrechoChan:  make(chan reqCancelarTrecho, 100),
		cancelarReservaChan: make(chan reqCancelarReserva, 100),
	} //inicialização do canal principal. o 100 ou 50 é uma recomendação da documentação do Go, para o buffer não travar imediatemente
	// obter um contexto no momento em que o sistema sobe
	// sendo dois monitores concorrentes mas independetes
	go g.monitorAutenticacao()
	go g.monitorCaronas()
	return g
}

// converte "EMP_B=http://empresa-b:8081,EMP_C=http://empresa-c:8082" em mapa
func parsePeersEnv(raw string) map[string]string {
	mapa := make(map[string]string)
	if strings.TrimSpace(raw) == "" {
		return mapa
	}
	partes := strings.Split(raw, ",")
	for _, p := range partes {
		kv := strings.SplitN(strings.TrimSpace(p), "=", 2)
		if len(kv) == 2 {
			url := strings.TrimRight(strings.TrimSpace(kv[1]), "/")
			if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
				url = "http://" + url
			}
			mapa[strings.TrimSpace(kv[0])] = url
		}
	}
	return mapa
}

func (g *GerenciadorCaronas) EmpresaID() string {
	return g.empresaID
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
	//estado privado, inpedindo que as thread externa toque nessa variaveis
	caronas := make([]dominio.Carona, 0)
	contadorID := 0
	var lamportTS uint64 = 1
	commitsAtivos := make(map[string]registroCommit)

	// recupera estado persistido em disco caso exista
	if g.arquivoEstado != "" {
		if b, err := os.ReadFile(g.arquivoEstado); err == nil {
			var est estadoPersistido
			if json.Unmarshal(b, &est) == nil {
				caronas = est.Caronas
				contadorID = est.ContadorID
				if est.LamportTS > lamportTS {
					lamportTS = est.LamportTS
				}
				// ao reiniciar apos falha, descarta pre-reservas nao confirmadas
				for i := range caronas {
					for j := range caronas[i].Trechos {
						caronas[i].Trechos[j].PreReservas = nil
					}
				}
			}
		}
	}

	salvarEstado := func() {
		if g.arquivoEstado == "" {
			return
		}
		est := estadoPersistido{
			EmpresaID:  g.empresaID,
			ContadorID: contadorID,
			LamportTS:  lamportTS,
			Caronas:    caronas,
		}
		if b, err := json.MarshalIndent(est, "", "  "); err == nil {
			tmp := g.arquivoEstado + ".tmp"
			if os.WriteFile(tmp, b, 0644) == nil {
				_ = os.Rename(tmp, g.arquivoEstado)
			}
		}
	}

	atualizarRelogio := func(remoto uint64) uint64 {
		if remoto > lamportTS {
			lamportTS = remoto
		}
		lamportTS++
		return lamportTS
	}

	tickerTTL := time.NewTicker(300 * time.Millisecond)
	defer tickerTTL.Stop()

	for {
		select {
		case <-tickerTTL.C:
			limparPreReservasExpiradas(caronas, time.Now())

		case req := <-g.clockChan:
			req.resp <- atualizarRelogio(req.lamportRem)

		case req := <-g.publicarChan:
			lamportTS++
			contadorID++
			req.carona.EmpresaID = g.empresaID
			req.carona.Id = fmt.Sprintf("%s-C%d", g.empresaID, contadorID)
			req.carona.Ativa = true
			caronas = append(caronas, req.carona)
			salvarEstado()
			req.resp <- req.carona.Id

		case req := <-g.caronasLocaisChan:
			ts := atualizarRelogio(req.lamportRem)
			limparPreReservasExpiradas(caronas, time.Now())
			copia := copiarCaronasFiltradas(caronas, req.data)
			req.resp <- dominio.RestCaronasResponse{
				EmpresaID: g.empresaID,
				LamportTS: ts,
				Caronas:   copia,
			}

		case req := <-g.preReservaChan:
			ts := atualizarRelogio(req.dados.LamportTS)
			limparPreReservasExpiradas(caronas, time.Now())
			ok, msg := executarPreReservaMemoria(caronas, req.dados, time.Now().Add(ttlPreReserva))
			req.resp <- dominio.RestStatusResponse{
				Ok:        ok,
				Msg:       msg,
				EmpresaID: g.empresaID,
				LamportTS: ts,
			}

		case req := <-g.confirmarChan:
			ts := atualizarRelogio(req.dados.LamportTS)
			limparPreReservasExpiradas(caronas, time.Now())
			ok, msg, reg, novoCommit := executarConfirmacaoMemoria(caronas, req.dados, commitsAtivos)
			if ok && novoCommit {
				commitsAtivos[req.dados.TxID+"|"+reg.CaronaID] = reg
				salvarEstado()
			}
			req.resp <- dominio.RestStatusResponse{
				Ok:        ok,
				Msg:       msg,
				EmpresaID: g.empresaID,
				LamportTS: ts,
			}

		case req := <-g.abortarChan:
			ts := atualizarRelogio(req.dados.LamportTS)
			alterou := executarAbortoMemoria(caronas, req.dados, commitsAtivos)
			if alterou {
				salvarEstado()
			}
			req.resp <- dominio.RestStatusResponse{
				Ok:        true,
				Msg:       "transacao abortada e assento liberado",
				EmpresaID: g.empresaID,
				LamportTS: ts,
			}

		case req := <-g.listarMotChan:
			req.resp <- listarMotoristaMemoria(caronas, req.motoristaID)

		case req := <-g.listarPassChan:
			req.resp <- listarPassageiroMemoria(caronas, req.passageiroID)

		case req := <-g.cancelarCaronaChan:
			ok := cancelarCaronaMemoria(caronas, req.caronaID, req.motoristaID, g.empresaID)
			if ok {
				salvarEstado()
			}
			req.resp <- ok

		case req := <-g.cancelarTrechoChan:
			ok := cancelarTrechoMemoria(caronas, req.caronaID, req.motoristaID, req.origem, req.destino, g.empresaID)
			if ok {
				salvarEstado()
			}
			req.resp <- ok

		case req := <-g.cancelarReservaChan:
			ok := cancelarReservaMemoria(caronas, req.caronaID, req.origem, req.destino, req.passageiroID, g.empresaID)
			if ok {
				salvarEstado()
			}
			req.resp <- ok
		}
	}
}

// funcao interna da memoria do monitoCaronas
func normalizarCaronaID(caronaID, empresaLocal string) string {
	if !strings.Contains(caronaID, "-") {
		return fmt.Sprintf("%s-%s", empresaLocal, caronaID)
	}
	return caronaID
}

func limparPreReservasExpiradas(caronas []dominio.Carona, agora time.Time) {
	for i := range caronas {
		for j := range caronas[i].Trechos {
			tr := &caronas[i].Trechos[j]
			if len(tr.PreReservas) == 0 {
				continue
			}
			ativas := tr.PreReservas[:0]
			for _, pr := range tr.PreReservas {
				if agora.Before(pr.ExpiraEm) {
					ativas = append(ativas, pr)
				}
			}
			tr.PreReservas = ativas
		}
	}
}

func copiarCaronasFiltradas(caronas []dominio.Carona, dataFiltro string) []dominio.Carona {
	var lista []dominio.Carona
	for _, c := range caronas {
		if !c.Ativa {
			continue
		}
		if dataFiltro != "" && c.DataPartida.Format("2006-01-02") != dataFiltro {
			continue
		}
		copiaTrechos := make([]dominio.TrechoCarona, len(c.Trechos))
		for i, tr := range c.Trechos {
			copiaTrechos[i] = tr
			// Na listagem publica para busca, considera pre-reservas ativas na ocupacao visivel
			ocupacaoTotal := tr.AssentosOcupados + len(tr.PreReservas)
			if ocupacaoTotal > c.AssentosDisponiveis {
				ocupacaoTotal = c.AssentosDisponiveis
			}
			copiaTrechos[i].AssentosOcupados = ocupacaoTotal
		}
		cCopia := c
		cCopia.Trechos = copiaTrechos
		lista = append(lista, cCopia)
	}
	return lista
}

// implementa o 2PC com preempcao por relogio de Lamport
func executarPreReservaMemoria(caronas []dominio.Carona, req dominio.RestPrepareRequest, expiraEm time.Time) (bool, string) {
	var caronaRef *dominio.Carona
	for i := range caronas {
		if caronas[i].Id == req.CaronaID || (!strings.Contains(req.CaronaID, "-") && strings.HasSuffix(caronas[i].Id, "-"+req.CaronaID)) {
			caronaRef = &caronas[i]
			break
		}
	}
	if caronaRef == nil || !caronaRef.Ativa {
		return false, "carona inexistente ou inativa"
	}

	idxOrigem, idxDestino := encontrarIndices(*caronaRef, req.Origem, req.Destino)
	if idxOrigem == -1 || idxDestino == -1 || idxOrigem > idxDestino {
		return false, "trecho invalido na carona"
	}

	novaPre := dominio.PreReserva{
		TxID:         req.TxID,
		PassageiroID: req.PassageiroID,
		EmpresaCoord: req.EmpresaCoord,
		LamportTS:    req.LamportTS,
		ExpiraEm:     expiraEm,
	}

	vitimasParaPreemptar := make(map[string]bool)

	// Verifica se todos os subtrechos possuem vaga livre ou permitem preempcao por Lamport
	for i := idxOrigem; i <= idxDestino; i++ {
		tr := &caronaRef.Trechos[i]
		if tr.Cancelado || tr.AssentosOcupados >= caronaRef.AssentosDisponiveis {
			return false, "assento definitivamente esgotado"
		}

		//caso o TxID repita o Prepare
		jaExiste := false
		for _, pr := range tr.PreReservas {
			if pr.TxID == req.TxID {
				jaExiste = true
				break
			}
		}
		if jaExiste {
			continue
		}

		if tr.AssentosOcupados+len(tr.PreReservas) < caronaRef.AssentosDisponiveis {
			continue
		}

		// Todos os assentos livres estao em PreReserva
		idxPior := -1
		for j, pr := range tr.PreReservas {
			if idxPior == -1 || tr.PreReservas[idxPior].TemPrecedenciaSobre(pr) {
				idxPior = j
			}
		}
		if idxPior != -1 && novaPre.TemPrecedenciaSobre(tr.PreReservas[idxPior]) {
			vitimasParaPreemptar[tr.PreReservas[idxPior].TxID] = true
		} else {
			return false, "conflito: assento bloqueado por transacao de maior precedencia"
		}
	}

	// Remove integralmente da carona quaisquer transacoes preemptadas
	if len(vitimasParaPreemptar) > 0 {
		for i := range caronaRef.Trechos {
			tr := &caronaRef.Trechos[i]
			restantes := tr.PreReservas[:0]
			for _, pr := range tr.PreReservas {
				if !vitimasParaPreemptar[pr.TxID] {
					restantes = append(restantes, pr)
				}
			}
			tr.PreReservas = restantes
		}
	}

	// Registra a nova pre-reserva nos trechos solicitados
	for i := idxOrigem; i <= idxDestino; i++ {
		tr := &caronaRef.Trechos[i]
		atualizado := false
		for j := range tr.PreReservas {
			if tr.PreReservas[j].TxID == req.TxID {
				tr.PreReservas[j] = novaPre
				atualizado = true
				break
			}
		}
		if !atualizado {
			tr.PreReservas = append(tr.PreReservas, novaPre)
		}
	}

	return true, "pre-reserva registrada"
}

// executarConfirmacaoMemoria converte a PreReserva em reserva definitiva
func executarConfirmacaoMemoria(caronas []dominio.Carona, req dominio.RestDecisionRequest, commits map[string]registroCommit) (bool, string, registroCommit, bool) {
	var caronaRef *dominio.Carona
	for i := range caronas {
		if caronas[i].Id == req.CaronaID || (!strings.Contains(req.CaronaID, "-") && strings.HasSuffix(caronas[i].Id, "-"+req.CaronaID)) {
			caronaRef = &caronas[i]
			break
		}
	}
	if caronaRef == nil || !caronaRef.Ativa {
		return false, "carona inexistente ou inativa", registroCommit{}, false
	}

	//se a confirmacao for repetida para o mesmo TxID, retorna OK sem duplicar
	if regExistente, jaComitado := commits[req.TxID+"|"+caronaRef.Id]; jaComitado {
		return true, "reserva ja confirmada anteriormente (idempotente)", regExistente, false
	}

	idxOrigem, idxDestino := encontrarIndices(*caronaRef, req.Origem, req.Destino)
	if idxOrigem == -1 || idxDestino == -1 || idxOrigem > idxDestino {
		return false, "trecho invalido", registroCommit{}, false
	}

	// Verifica se a PreReserva continua ativa em todos os subtrechos e se ha capacidade
	for i := idxOrigem; i <= idxDestino; i++ {
		tr := &caronaRef.Trechos[i]
		if tr.Cancelado || tr.AssentosOcupados >= caronaRef.AssentosDisponiveis {
			return false, "assento indisponivel no momento do commit", registroCommit{}, false
		}
		encontrouPre := false
		for _, pr := range tr.PreReservas {
			if pr.TxID == req.TxID {
				encontrouPre = true
				break
			}
		}
		if !encontrouPre {
			return false, "pre-reserva expirada ou preemptada por outra transacao", registroCommit{}, false
		}
	}

	// Consome a PreReserva e incrementa AssentosOcupados definitivamente
	for i := idxOrigem; i <= idxDestino; i++ {
		tr := &caronaRef.Trechos[i]
		restantes := tr.PreReservas[:0]
		for _, pr := range tr.PreReservas {
			if pr.TxID != req.TxID {
				restantes = append(restantes, pr)
			}
		}
		tr.PreReservas = restantes
		tr.AssentosOcupados++
		tr.Passageiros = append(tr.Passageiros, req.PassageiroID)
	}

	return true, "reserva confirmada", registroCommit{
		CaronaID:     caronaRef.Id,
		IdxOrigem:    idxOrigem,
		IdxDestino:   idxDestino,
		PassageiroID: req.PassageiroID,
	}, true
}

// remove PreReserva ou desfaz Commit caso acionado em rollback
func executarAbortoMemoria(caronas []dominio.Carona, req dominio.RestDecisionRequest, commits map[string]registroCommit) bool {
	alterou := false
	for i := range caronas {
		if caronas[i].Id == req.CaronaID || (!strings.Contains(req.CaronaID, "-") && strings.HasSuffix(caronas[i].Id, "-"+req.CaronaID)) {
			// Remove qualquer pre-reserva da transacao
			for j := range caronas[i].Trechos {
				tr := &caronas[i].Trechos[j]
				restantes := tr.PreReservas[:0]
				for _, pr := range tr.PreReservas {
					if pr.TxID != req.TxID {
						restantes = append(restantes, pr)
					}
				}
				tr.PreReservas = restantes
			}

			// Se havia sido comitada antes de uma falha na Fase 2 de outro servidor, reverte
			chave := req.TxID + "|" + caronas[i].Id
			chaveCurta := req.TxID + "|" + req.CaronaID
			reg, existe := commits[chave]
			if !existe {
				reg, existe = commits[chaveCurta]
			}
			if existe {
				for j := reg.IdxOrigem; j <= reg.IdxDestino; j++ {
					tr := &caronas[i].Trechos[j]
					if tr.AssentosOcupados > 0 {
						tr.AssentosOcupados--
					}
					removido := false
					novaLista := make([]string, 0, len(tr.Passageiros))
					for _, p := range tr.Passageiros {
						if !removido && p == reg.PassageiroID {
							removido = true
							continue
						}
						novaLista = append(novaLista, p)
					}
					tr.Passageiros = novaLista
				}
				delete(commits, chave)
				delete(commits, chaveCurta)
				alterou = true
			}
			break
		}
	}
	return alterou
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

func (g *GerenciadorCaronas) ObterCaronasLocais(data string, lamportRemoto uint64) dominio.RestCaronasResponse {
	resp := make(chan dominio.RestCaronasResponse)
	g.caronasLocaisChan <- reqCaronasLocais{data: data, lamportRem: lamportRemoto, resp: resp}
	return <-resp
}

func (g *GerenciadorCaronas) PreReservarLocal(req dominio.RestPrepareRequest) dominio.RestStatusResponse {
	resp := make(chan dominio.RestStatusResponse)
	g.preReservaChan <- reqPreReserva{dados: req, resp: resp}
	return <-resp
}

func (g *GerenciadorCaronas) ConfirmarReservaLocal(req dominio.RestDecisionRequest) dominio.RestStatusResponse {
	resp := make(chan dominio.RestStatusResponse)
	g.confirmarChan <- reqDecisaoReserva{dados: req, resp: resp}
	return <-resp
}

func (g *GerenciadorCaronas) AbortarReservaLocal(req dominio.RestDecisionRequest) dominio.RestStatusResponse {
	resp := make(chan dominio.RestStatusResponse)
	g.abortarChan <- reqDecisaoReserva{dados: req, resp: resp}
	return <-resp
}

func (g *GerenciadorCaronas) avancarRelogio(remoto uint64) uint64 {
	resp := make(chan uint64)
	g.clockChan <- reqClock{lamportRem: remoto, resp: resp}
	return <-resp
}

// adaptado para coordenar reserva simples e composta
// feito para aceitar o ID de carona simples ou composto, como tem no API REST
func (g *GerenciadorCaronas) Reservar(caronaOuItinerarioID, origem, destino, passageiroID string) bool {
	itens := g.interpretarItensReserva(caronaOuItinerarioID, origem, destino)
	if len(itens) == 0 {
		return false
	}
	return g.executar2PC(passageiroID, itens)
}

func (g *GerenciadorCaronas) interpretarItensReserva(idBruto, origemPadrao, destinoPadrao string) []ItemReserva {
	partes := strings.Split(idBruto, "+")
	var itens []ItemReserva

	for _, parte := range partes {
		parte = strings.TrimSpace(parte)
		if parte == "" {
			continue
		}
		caronaID := parte
		orig := origemPadrao
		dest := destinoPadrao

		// formato composto: EMP_A-C1@Salvador>Feira
		if idxArr := strings.Index(parte, "@"); idxArr != -1 {
			caronaID = parte[:idxArr]
			rota := parte[idxArr+1:]
			cidades := strings.SplitN(rota, ">", 2)
			if len(cidades) == 2 {
				orig = cidades[0]
				dest = cidades[1]
			}
		}

		empID := g.empresaID
		if idxHifen := strings.Index(caronaID, "-"); idxHifen != -1 {
			empID = caronaID[:idxHifen]
		} else {
			caronaID = fmt.Sprintf("%s-%s", g.empresaID, caronaID)
		}

		itens = append(itens, ItemReserva{
			EmpresaID: empID,
			CaronaID:  caronaID,
			Origem:    orig,
			Destino:   dest,
		})
	}
	return itens
}

// executar2PC atua como coordenador para confirmar ou abortar atomicamente o itinerario
func (g *GerenciadorCaronas) executar2PC(passageiroID string, itens []ItemReserva) bool {
	// Prevencao de Deadlock Distribuido
	// ordena canonicamente os trechos por EmpresaID e CaronaID para impedir espera circular
	// quando dois passageiros reservam os mesmos trechos em ordens distintas
	sort.Slice(itens, func(i, j int) bool {
		if itens[i].EmpresaID != itens[j].EmpresaID {
			return itens[i].EmpresaID < itens[j].EmpresaID
		}
		return itens[i].CaronaID < itens[j].CaronaID
	})
	ts := g.avancarRelogio(0)
	txID := fmt.Sprintf("TX-%s-%d-%d", g.empresaID, ts, time.Now().UnixNano())

	//fase de pre reservar
	for _, item := range itens {
		reqPrep := dominio.RestPrepareRequest{
			TxID:         txID,
			CaronaID:     item.CaronaID,
			Origem:       item.Origem,
			Destino:      item.Destino,
			PassageiroID: passageiroID,
			EmpresaCoord: g.empresaID,
			LamportTS:    ts,
		}

		var status dominio.RestStatusResponse
		if item.EmpresaID == g.empresaID {
			status = g.PreReservarLocal(reqPrep)
		} else {
			var err error
			status, err = g.enviarPostConsorcio(item.EmpresaID, "/consorcio/reservas/preparar", reqPrep)
			if err != nil {
				status.Ok = false
			} else {
				g.avancarRelogio(status.LamportTS)
			}
		}

		if !status.Ok {
			// qualquer recusa ou falha de rede aborta imediatamente todos os trechos
			g.abortarTodos(txID, passageiroID, itens)
			return false
		}
	}

	// fase de confirmacao (COMMIT)
	tsCommit := g.avancarRelogio(0)
	for _, item := range itens {
		reqDec := dominio.RestDecisionRequest{
			TxID:         txID,
			CaronaID:     item.CaronaID,
			Origem:       item.Origem,
			Destino:      item.Destino,
			PassageiroID: passageiroID,
			EmpresaCoord: g.empresaID,
			LamportTS:    tsCommit,
		}

		var status dominio.RestStatusResponse
		if item.EmpresaID == g.empresaID {
			status = g.ConfirmarReservaLocal(reqDec)
		} else {
			var err error
			status, err = g.enviarPostConsorcio(item.EmpresaID, "/consorcio/reservas/confirmar", reqDec)
			if err != nil {
				status.Ok = false
			} else {
				g.avancarRelogio(status.LamportTS)
			}
		}

		if !status.Ok {
			// se perdeu pre-reserva por queda, desfaz inclusive o que ja comitou
			g.abortarTodos(txID, passageiroID, itens)
			return false
		}
	}

	return true
}

func (g *GerenciadorCaronas) abortarTodos(txID, passageiroID string, itens []ItemReserva) {
	tsAbort := g.avancarRelogio(0)
	for _, item := range itens {
		reqAbort := dominio.RestDecisionRequest{
			TxID:         txID,
			CaronaID:     item.CaronaID,
			Origem:       item.Origem,
			Destino:      item.Destino,
			PassageiroID: passageiroID,
			EmpresaCoord: g.empresaID,
			LamportTS:    tsAbort,
		}
		if item.EmpresaID == g.empresaID {
			g.AbortarReservaLocal(reqAbort)
		} else {
			if st, err := g.enviarPostConsorcio(item.EmpresaID, "/consorcio/reservas/abortar", reqAbort); err == nil {
				g.avancarRelogio(st.LamportTS)
			}
		}
	}
}

func (g *GerenciadorCaronas) enviarPostConsorcio(empresaDestino, rota string, payload interface{}) (dominio.RestStatusResponse, error) {
	baseURL, existe := g.peers[empresaDestino]
	if !existe {
		return dominio.RestStatusResponse{Ok: false, Msg: "empresa desconhecida"}, fmt.Errorf("peer %s nao configurado", empresaDestino)
	}

	corpo, err := json.Marshal(payload)
	if err != nil {
		return dominio.RestStatusResponse{Ok: false}, err
	}

	resp, err := g.httpClient.Post(baseURL+rota, "application/json", bytes.NewReader(corpo))
	if err != nil {
		return dominio.RestStatusResponse{Ok: false, Msg: "timeout ou falha de conexao"}, err
	}
	defer resp.Body.Close()

	var resultado dominio.RestStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&resultado); err != nil {
		return dominio.RestStatusResponse{Ok: false}, err
	}
	if resp.StatusCode >= 400 {
		resultado.Ok = false
	}
	return resultado, nil
}

// busca com varias empresas
func (g *GerenciadorCaronas) BuscarItinerarios(origem, destino string, data time.Time) []ItinerarioComposto {
	dataStr := data.Format("2006-01-02")
	locais := g.ObterCaronasLocais(dataStr, 0)
	todasCaronas := make([]dominio.Carona, 0, len(locais.Caronas))
	todasCaronas = append(todasCaronas, locais.Caronas...)

	// Consulta concorrentemente todas as empresas parceiras com timeout curto
	type resPeer struct {
		resp dominio.RestCaronasResponse
		ok   bool
	}
	chPeers := make(chan resPeer, len(g.peers))
	for _, baseURL := range g.peers {
		go func(url string) {
			cliente := &http.Client{Timeout: 800 * time.Millisecond}
			r, err := cliente.Get(fmt.Sprintf("%s/consorcio/caronas?data=%s", url, dataStr))
			if err != nil {
				chPeers <- resPeer{ok: false}
				return
			}
			defer r.Body.Close()
			if r.StatusCode != http.StatusOK {
				chPeers <- resPeer{ok: false}
				return
			}
			var dados dominio.RestCaronasResponse
			if err := json.NewDecoder(r.Body).Decode(&dados); err != nil {
				chPeers <- resPeer{ok: false}
				return
			}
			chPeers <- resPeer{resp: dados, ok: true}
		}(baseURL)
	}

	for i := 0; i < len(g.peers); i++ {
		res := <-chPeers
		if res.ok {
			g.avancarRelogio(res.resp.LamportTS)
			todasCaronas = append(todasCaronas, res.resp.Caronas...)
		}
	}

	return buscarGrafoFederado(todasCaronas, origem, destino, data)
}

// representa uma viagem continua dentro de uma mesma carona entre duas cidades.
type pernaCarona struct {
	EmpresaID     string
	CaronaID      string
	MotoristaID   string
	CidadeOrigem  string
	CidadeDestino string
	Subtrechos    []TrechoViagem
	ValorSubTotal float64
}

// executa Busca em Largura (BFS) para montar itinerarios diretos e compostos por multiplas conexoes (ate 4 empresas/motoristas)
func buscarGrafoFederado(caronas []dominio.Carona, origem, destino string, data time.Time) []ItinerarioComposto {
	dataAlvo := data.Format("2006-01-02")
	adjacencia := make(map[string][]pernaCarona)

	for _, c := range caronas {
		if !c.Ativa || c.DataPartida.Format("2006-01-02") != dataAlvo {
			continue
		}
		empID := c.EmpresaID
		if empID == "" && strings.Contains(c.Id, "-") {
			empID = strings.SplitN(c.Id, "-", 2)[0]
		}

		// gera todas as combinacoes validas de embarque (i) e desembarque (j) dentro da carona c
		for i := 0; i < len(c.Trechos); i++ {
			var sub []TrechoViagem
			var soma float64
			for j := i; j < len(c.Trechos); j++ {
				tr := c.Trechos[j]
				if tr.Cancelado || tr.AssentosOcupados >= c.AssentosDisponiveis {
					break // se um trecho intermediario estiver lotado, nao e possivel seguir adiante nesta carona
				}
				sub = append(sub, TrechoViagem{
					EmpresaID:     empID,
					CaronaID:      c.Id,
					MotoristaID:   c.MotoristaID,
					CidadeOrigem:  tr.CidadeOrigem,
					CidadeDestino: tr.CidadeDestino,
					Valor:         c.ValorPorTrecho,
				})
				soma += c.ValorPorTrecho

				copiaSub := make([]TrechoViagem, len(sub))
				copy(copiaSub, sub)
				cidOrig := c.Trechos[i].CidadeOrigem
				cidDest := tr.CidadeDestino

				adjacencia[cidOrig] = append(adjacencia[cidOrig], pernaCarona{
					EmpresaID:     empID,
					CaronaID:      c.Id,
					MotoristaID:   c.MotoristaID,
					CidadeOrigem:  cidOrig,
					CidadeDestino: cidDest,
					Subtrechos:    copiaSub,
					ValorSubTotal: soma,
				})
			}
		}
	}

	type estadoBFS struct {
		cidadeAtual string
		pernas      []pernaCarona
		visitadas   map[string]bool
		caronasUso  map[string]bool
	}

	fila := []estadoBFS{{
		cidadeAtual: origem,
		pernas:      nil,
		visitadas:   map[string]bool{origem: true},
		caronasUso:  map[string]bool{},
	}}

	var resultados []ItinerarioComposto
	const maxPernas = 4

	for len(fila) > 0 {
		atual := fila[0]
		fila = fila[1:]

		if len(atual.pernas) >= maxPernas {
			continue
		}

		for _, perna := range adjacencia[atual.cidadeAtual] {
			if atual.visitadas[perna.CidadeDestino] || atual.caronasUso[perna.CaronaID] {
				continue
			}

			novasPernas := make([]pernaCarona, len(atual.pernas)+1)
			copy(novasPernas, atual.pernas)
			novasPernas[len(atual.pernas)] = perna

			if perna.CidadeDestino == destino {
				var todosTrechos []TrechoViagem
				var valorTotal float64
				var partesID []string

				for _, p := range novasPernas {
					todosTrechos = append(todosTrechos, p.Subtrechos...)
					valorTotal += p.ValorSubTotal
					partesID = append(partesID, fmt.Sprintf("%s@%s>%s", p.CaronaID, p.CidadeOrigem, p.CidadeDestino))
				}

				itinID := strings.Join(partesID, "+")
				if len(novasPernas) == 1 {
					itinID = novasPernas[0].CaronaID
				}

				resultados = append(resultados, ItinerarioComposto{
					ItinerarioID: itinID,
					Trechos:      todosTrechos,
					ValorTotal:   valorTotal,
					Baldeacoes:   len(novasPernas) - 1,
				})
				continue
			}

			novasVisitadas := make(map[string]bool, len(atual.visitadas)+1)
			for k, v := range atual.visitadas {
				novasVisitadas[k] = v
			}
			novasVisitadas[perna.CidadeDestino] = true

			novasCaronas := make(map[string]bool, len(atual.caronasUso)+1)
			for k, v := range atual.caronasUso {
				novasCaronas[k] = v
			}
			novasCaronas[perna.CaronaID] = true

			fila = append(fila, estadoBFS{
				cidadeAtual: perna.CidadeDestino,
				pernas:      novasPernas,
				visitadas:   novasVisitadas,
				caronasUso:  novasCaronas,
			})
		}
	}

	return resultados
}

// Listagem e cancelamento

func (g *GerenciadorCaronas) ListarPorMotorista(motoristaID string) []dominio.Carona {
	resposta := make(chan []dominio.Carona)
	g.listarMotChan <- reqListarMotorista{motoristaID: motoristaID, resp: resposta}
	return <-resposta
}

func (g *GerenciadorCaronas) ListarPorPassageiro(passageiroID string) []MinhaViagem {
	resposta := make(chan []MinhaViagem)
	g.listarPassChan <- reqListarPassageiro{passageiroID: passageiroID, resp: resposta}
	viagens := <-resposta

	// Agrega tambem viagens reservadas pelo passageiro nos demais servidores do consorcio
	for _, baseURL := range g.peers {
		cliente := &http.Client{Timeout: 800 * time.Millisecond}
		r, err := cliente.Get(fmt.Sprintf("%s/consorcio/caronas", baseURL))
		if err != nil {
			continue
		}
		var dados dominio.RestCaronasResponse
		if json.NewDecoder(r.Body).Decode(&dados) == nil {
			viagensRemotas := listarPassageiroMemoria(dados.Caronas, passageiroID)
			viagens = append(viagens, viagensRemotas...)
		}
		r.Body.Close()
	}
	return viagens
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

func (g *GerenciadorCaronas) CancelarReserva(caronaID, origem, destino, passageiroID string) bool {
	if strings.Contains(caronaID, "-") {
		empAlvo := strings.SplitN(caronaID, "-", 2)[0]
		if empAlvo != g.empresaID {
			reqAbort := dominio.RestDecisionRequest{
				TxID:         "CANCEL-" + passageiroID,
				CaronaID:     caronaID,
				Origem:       origem,
				Destino:      destino,
				PassageiroID: passageiroID,
				EmpresaCoord: g.empresaID,
				LamportTS:    g.avancarRelogio(0),
			}
			st, err := g.enviarPostConsorcio(empAlvo, "/consorcio/reservas/cancelar", reqAbort)
			return err == nil && st.Ok
		}
	}
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

func cancelarCaronaMemoria(caronas []dominio.Carona, caronaID, motoristaID, empLocal string) bool {
	idAlvo := normalizarCaronaID(caronaID, empLocal)
	for i := range caronas {
		if (caronas[i].Id == caronaID || caronas[i].Id == idAlvo) && caronas[i].MotoristaID == motoristaID {
			caronas[i].Ativa = false
			return true
		}
	}
	return false
}

func cancelarTrechoMemoria(caronas []dominio.Carona, caronaID, motoristaID, origem, destino, empLocal string) bool {
	idAlvo := normalizarCaronaID(caronaID, empLocal)
	for i := range caronas {
		if (caronas[i].Id == caronaID || caronas[i].Id == idAlvo) && caronas[i].MotoristaID == motoristaID {
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

func cancelarReservaMemoria(caronas []dominio.Carona, caronaID, origem, destino, passageiroID, empLocal string) bool {
	idAlvo := normalizarCaronaID(caronaID, empLocal)
	var caronaRef *dominio.Carona
	for i := range caronas {
		if caronas[i].Id == caronaID || caronas[i].Id == idAlvo {
			caronaRef = &caronas[i]
			break
		}
	}
	if caronaRef == nil {
		return false
	}

	idxOrigem, idxDestino := encontrarIndices(*caronaRef, origem, destino)
	if idxOrigem == -1 || idxDestino == -1 || idxOrigem > idxDestino {
		return false
	}

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

	for i := idxOrigem; i <= idxDestino; i++ {
		trecho := &caronaRef.Trechos[i]
		if trecho.AssentosOcupados > 0 {
			trecho.AssentosOcupados--
		}
		var novaLista []string
		removido := false
		for _, p := range trecho.Passageiros {
			if !removido && p == passageiroID {
				removido = true
				continue
			}
			novaLista = append(novaLista, p)
		}
		trecho.Passageiros = novaLista
	}
	return true
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
