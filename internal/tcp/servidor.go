package tcp

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"time"
	"vumbora/internal/dominio"
	"vumbora/internal/roteamento"
)

// Envelopamento do protocolo
type Request struct {
	Op   string          `json:"op"`
	Data json.RawMessage `json:"data"` //payload especifico, desacopla o evelope do conteudo
}

// padronizar a resposta para cliente
type Response struct {
	//as variaveis aqui não precisa ser tão descritiva igual java, cuidado com o costume de liguagem
	Ok  bool            `json:"ok"`
	Msg string          `json:"msg"`
	Res json.RawMessage `json:"res,omitempty"`
}

// agrupar o listener
type Server struct {
	addr string
	m    *roteamento.GerenciadorCaronas
}

// apelido do servidor tcp.new
func NovoServidor(addr string, m *roteamento.GerenciadorCaronas) *Server {
	return &Server{
		addr: addr,
		m:    m,
	}
}

// start no socket para começar as conexoes
func (srv *Server) Iniciar() error {
	l, err := net.Listen("tcp", srv.addr)
	if err != nil {
		return err
	}
	//defer fecha e garante que o socket esteja fechado no fin da função
	defer l.Close()
	fmt.Printf("[TCP] Servidor Vumbora escutando em %s\n", srv.addr)

	for {
		//laço de escuta contínuo aguardando conexões
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("Erro na conexao %s", err)
			continue
		}
		//criar uma goroutime somente para esse cliente
		go srv.handle(conn)
	}
}

// Dedolver os dados ao cliente
func (srv *Server) reply(conn net.Conn, res Response) {
	b, _ := json.Marshal(res)
	conn.Write(append(b, '\n'))
}

func (srv *Server) handle(conn net.Conn) {
	defer conn.Close() //liberação do descritor de arquivos

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		linha := scanner.Text()
		var request Request
		if err := json.Unmarshal([]byte(linha), &request); err != nil {
			srv.reply(conn, Response{Ok: false, Msg: "JSON malformatado"})
			continue
		}

		// testar se de erro fmt.Printf("[TCP] Comando recebido: %s\n", request.Op)

		//roteador de operacoes
		switch request.Op {
		//os bytes do RawMessage são desseralizados aqui para a struct especifica
		case "LOGIN":
			var p struct {
				Email string `json:"email"`
				Senha string `json:"senha"`
			}
			//json.Unmarshal vai verificar o dado e se tiver dado mal formado, tipo de dado incompativel ou faltar campo, vai retorna um erro
			//o servidor não derruba o goroutine, ele captura a falha e envia uma resposta estruturada, mantedo a conexão para proxima requisição
			if err := json.Unmarshal(request.Data, &p); err != nil {
				srv.reply(conn, Response{Ok: false, Msg: "dados de login invalidos"})
				continue
			}

			if srv.m.Autenticar(p.Email, p.Senha) {
				srv.reply(conn, Response{Ok: true, Msg: "autenticado com sucesso"})
			} else {
				srv.reply(conn, Response{Ok: false, Msg: "senha incorreta"})
			}
		case "BUSCAR":
			var payload struct {
				Origem  string `json:"origem"`
				Destino string `json:"destino"`
				Data    string `json:"data"`
			}
			//converte Data para struct de Busca
			if err := json.Unmarshal(request.Data, &payload); err != nil {
				srv.reply(conn, Response{Ok: false, Msg: "dados de busca invalido"})
				continue
			}

			// Aceita tanto YYYY-MM-DD quanto RFC3339
			dt, err := time.Parse("2006-01-02", payload.Data)
			if err != nil {
				dt, err = time.Parse(time.RFC3339, payload.Data)
			}
			if err != nil {
				srv.reply(conn, Response{Ok: false, Msg: "formato de data invalido, use DD-MM"})
				continue
			}

			itinerarios := srv.m.BuscarItinerarios(payload.Origem, payload.Destino, dt)
			resBytes, _ := json.Marshal(itinerarios)
			srv.reply(conn, Response{Ok: true, Msg: "busca concluida", Res: resBytes})
		case "RESERVAR":
			var payload struct {
				CaronaID     string `json:"caronaid"`
				Origem       string `json:"origem"`
				Destino      string `json:"destino"`
				PassageiroID string `json:"passageiroid"`
			}
			//valida o formato e tipagem dos dados
			if err := json.Unmarshal(request.Data, &payload); err != nil {
				srv.reply(conn, Response{Ok: false, Msg: "dados da reserva invalido"})
				continue //nao derruba a conexao, apenas rejeita o pacote
			}
			if srv.m.Reservar(payload.CaronaID, payload.Origem, payload.Destino, payload.PassageiroID) {
				srv.reply(conn, Response{Ok: true, Msg: "reserva confirmada"})
			} else { // verificação para inpedir do motorista fazer reserva em assento indisponivel
				srv.reply(conn, Response{Ok: false, Msg: "assento indisponivel"})
			}
		case "PUBLICAR":
			//struct anonima, mapear o json vindo do cliente
			var p struct {
				MotoristaID string   `json:"motoristaid"`
				Data        string   `json:"data"`
				Preco       float64  `json:"preco"`
				Assentos    int      `json:"assentos"`
				Rota        []string `json:"rota"`
			}
			if err := json.Unmarshal(request.Data, &p); err != nil {
				srv.reply(conn, Response{Ok: false, Msg: "dados invalidos"})
				continue
			}

			dt, err := time.Parse("2006-01-02", p.Data)
			if err != nil {
				srv.reply(conn, Response{Ok: false, Msg: "data invalida, use AAAA-MM-DD"})
				continue
			}

			//converter rota em trechos
			var trechos []dominio.TrechoCarona
			for i := 0; i < len(p.Rota)-1; i++ {
				trechos = append(trechos, dominio.TrechoCarona{
					CidadeOrigem:  p.Rota[i],
					CidadeDestino: p.Rota[i+1],
				})
			}
			c := dominio.Carona{
				MotoristaID:         p.MotoristaID,
				DataPartida:         dt,
				ValorPorTrecho:      p.Preco,
				AssentosDisponiveis: p.Assentos,
				Trechos:             trechos,
			}
			// O monitor gera o ID e anexa a carona atomicamente via canal
			idGerado := srv.m.PublicarCarona(c)
			srv.reply(conn, Response{Ok: true, Msg: fmt.Sprintf("carona %s publicada com sucesso", idGerado)})

		case "MINHASCARONAS":
			var p struct {
				MotoristaID string `json:"motoristaid"`
			}
			if err := json.Unmarshal(request.Data, &p); err != nil {
				srv.reply(conn, Response{Ok: false, Msg: "dados invalidos"})
				continue
			}

			caronas := srv.m.ListarPorMotorista(p.MotoristaID)
			resBytes, _ := json.Marshal(caronas)
			srv.reply(conn, Response{Ok: true, Msg: "consulta realizada", Res: resBytes})
		case "MINHASVIAGENS":
			var p struct {
				PassageiroID string `json:"passageiroid"`
			}
			json.Unmarshal(request.Data, &p)
			viagens := srv.m.ListarPorPassageiro(p.PassageiroID)
			resBytes, _ := json.Marshal(viagens)
			srv.reply(conn, Response{Ok: true, Msg: "consulta realizada", Res: resBytes})
		case "CANCELAR":
			var p struct {
				CaronaID    string `json:"caronaid"`
				MotoristaID string `json:"motoristaid"`
			}
			if err := json.Unmarshal(request.Data, &p); err != nil {
				srv.reply(conn, Response{Ok: false, Msg: "dados invalidos"})
				continue
			}

			if srv.m.CancelarCarona(p.CaronaID, p.MotoristaID) {
				srv.reply(conn, Response{Ok: true, Msg: "carona cancelada com sucesso"})
			} else {
				srv.reply(conn, Response{Ok: false, Msg: "carona nao encontrada ou sem permissao"})
			}
		case "CANCELARTRECHO":
			var p struct {
				CaronaID    string `json:"caronaid"`
				MotoristaID string `json:"motoristaid"`
				Origem      string `json:"origem"`
				Destino     string `json:"destino"`
			}
			json.Unmarshal(request.Data, &p)
			if srv.m.CancelarTrecho(p.CaronaID, p.MotoristaID, p.Origem, p.Destino) {
				srv.reply(conn, Response{Ok: true, Msg: "trecho cancelado"})
			} else {
				srv.reply(conn, Response{Ok: false, Msg: "falha ao cancelar trecho"})
			}
		case "CANCELARRESERVA":
			var p struct {
				CaronaID     string `json:"caronaid"`
				Origem       string `json:"origem"`
				Destino      string `json:"destino"`
				PassageiroID string `json:"passageiroid"`
			}
			if err := json.Unmarshal(request.Data, &p); err != nil {
				srv.reply(conn, Response{Ok: false, Msg: "dados invalidos"})
				continue
			}

			if srv.m.CancelarReserva(p.CaronaID, p.Origem, p.Destino, p.PassageiroID) {
				srv.reply(conn, Response{Ok: true, Msg: "reserva cancelada com sucesso"})
			} else {
				srv.reply(conn, Response{Ok: false, Msg: "reserva nao encontrada"})
			}
		case "PING":
			srv.reply(conn, Response{Ok: true, Msg: "PONG"})
		default:
			srv.reply(conn, Response{Ok: false, Msg: "opção indiponivel"})

		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("[TCP] Erro na conexao com cliente: %v\n", err)
	}
}
