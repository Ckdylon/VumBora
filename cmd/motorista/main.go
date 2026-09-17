package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"vumbora/internal/dominio"
)

// evelopamento dos dados
// op define o indetificador remoto da solicitação
// data payload com parametros tipaods da operação
type Request struct {
	Op   string          `json:"op"`
	Data json.RawMessage `json:"data"`
}

// evelope resposta
// ok idica sucesso ou falha na transação
// msg mesagem que traz informação para verificar ou exibir na tela
// res dados que retorna da consulta
type Response struct {
	Ok  bool            `json:"ok"`
	Msg string          `json:"msg"`
	Res json.RawMessage `json:"res,omitempty"`
}

func main() {
	enderecoServidor := os.Getenv("SERVER_ADDR")
	if enderecoServidor == "" {
		enderecoServidor = "localhost:8080" // se estiver vazio permite eu testar localmente
	}

	conn, err := net.Dial("tcp", enderecoServidor)
	if err != nil {
		fmt.Printf("erro ao conectar: %v\n", err)
		return
	}
	defer conn.Close()

	in := bufio.NewReader(os.Stdin)
	out := bufio.NewReader(conn)

	fmt.Println("------ VUMBORA - Motorista -----")
	motoristaEmail := autenticar(conn, in, out)

	for {
		fmt.Println("\nPAINEL DO MOTORISTA (" + motoristaEmail + ")")
		fmt.Println("[1] Publicar Carona")
		fmt.Println("[2] Minhas Caronas e Faturamento")
		fmt.Println("[3] Cancelar Carona Inteira")
		fmt.Println("[4] Cancelar Apenas um Trecho")
		fmt.Println("[5] Sair")
		fmt.Print("Escolha: ")

		opt, _ := in.ReadString('\n')
		opt = strings.TrimSpace(opt)

		switch opt {
		case "1":
			publicarCarona(conn, in, out, motoristaEmail)
		case "2":
			listarCaronas(conn, out, motoristaEmail)
		case "3":
			cancelarCarona(conn, in, out, motoristaEmail)
		case "4":
			cancelarTrecho(conn, in, out, motoristaEmail)
		case "5":
			return

		default:
			fmt.Println("Opção inválida.")
		}
	}
}

func autenticar(conn net.Conn, in, out *bufio.Reader) string {
	for {
		fmt.Println("\n--- Autenticação ---")
		fmt.Print("Email: ")
		email, _ := in.ReadString('\n')
		email = strings.TrimSpace(email)

		fmt.Print("Senha: ")
		senha, _ := in.ReadString('\n')
		senha = strings.TrimSpace(senha)

		payload := map[string]string{
			"email": email,
			"senha": senha,
		}
		payloadBytes, _ := json.Marshal(payload)

		req := Request{Op: "LOGIN", Data: payloadBytes}
		reqBytes, _ := json.Marshal(req)
		conn.Write(append(reqBytes, '\n'))

		resStr, err := out.ReadString('\n')
		if err != nil {
			fmt.Println("Erro de rede ao logar.")
			continue
		}

		var res Response
		json.Unmarshal([]byte(resStr), &res)
		if res.Ok {
			fmt.Println("Acesso liberado")
			return email
		}
		fmt.Printf("%s. Tente novamente.\n", res.Msg)
	}
}
func publicarCarona(conn net.Conn, in, out *bufio.Reader, motoristaID string) {
	fmt.Print("Data da Partida(Mes-Dia): ")
	dataEntrada, _ := in.ReadString('\n')
	data := "2026-" + strings.TrimSpace(dataEntrada)
	fmt.Print("Preço por Trecho: ")
	precoStr, _ := in.ReadString('\n')
	preco, _ := strconv.ParseFloat(strings.TrimSpace(precoStr), 64)
	fmt.Print("Assentos Disponíveis: ")
	assentosStr, _ := in.ReadString('\n')
	assentos, _ := strconv.Atoi(strings.TrimSpace(assentosStr))
	//testar cidades separadas por virgula, parece melhor de trabalhar
	fmt.Print("Rota (Cidades separadas por vírgula.): ")
	rotaStr, _ := in.ReadString('\n')

	//função para limpar e dividir as cidade por virgula
	var rota []string
	for _, c := range strings.Split(strings.TrimSpace(rotaStr), ",") {
		if cLimpa := strings.TrimSpace(c); cLimpa != "" {
			rota = append(rota, cLimpa)
		}
	}

	payload := struct {
		MotoristaID string   `json:"motoristaid"`
		Data        string   `json:"data"`
		Preco       float64  `json:"preco"`
		Assentos    int      `json:"assentos"`
		Rota        []string `json:"rota"`
	}{
		MotoristaID: strings.TrimSpace(motoristaID),
		Data:        strings.TrimSpace(data),
		Preco:       preco,
		Assentos:    assentos,
		Rota:        rota,
	}

	payloadBytes, _ := json.Marshal(payload)
	req := Request{Op: "PUBLICAR", Data: payloadBytes}
	reqBytes, _ := json.Marshal(req)
	conn.Write(append(reqBytes, '\n'))

	resStr, err := out.ReadString('\n')
	if err != nil {
		fmt.Printf("erro na rede: %v\n", err)
		return
	}

	var res Response
	json.Unmarshal([]byte(resStr), &res)

	if res.Ok {
		fmt.Println("Carona publicada")
	} else {
		fmt.Printf("Falha ao publicar: %s\n", res.Msg)
	}
}

func listarCaronas(conn net.Conn, out *bufio.Reader, motoristaEmail string) {
	payload, _ := json.Marshal(map[string]string{"motoristaid": motoristaEmail})
	req, _ := json.Marshal(Request{Op: "MINHASCARONAS", Data: payload})
	conn.Write(append(req, '\n'))

	resStr, err := out.ReadString('\n')
	if err != nil {
		fmt.Println("Erro de rede.")
		return
	}

	var res Response
	json.Unmarshal([]byte(resStr), &res)

	var caronas []dominio.Carona
	json.Unmarshal(res.Res, &caronas)

	if len(caronas) == 0 {
		fmt.Println("Nenhuma carona ativa encontrada.")
		return
	}

	for _, c := range caronas {
		totalGanhos := 0.0
		fmt.Printf("\n========================================\n")
		fmt.Printf("Carona ID: %s | Partida: %s\n", c.Id, c.DataPartida.Format("02/01/2006"))
		fmt.Println("Trechos:")

		for _, tr := range c.Trechos {
			status := "Ativo"
			if tr.Cancelado {
				status = "CANCELADO"
			}
			ganhoTrecho := float64(tr.AssentosOcupados) * c.ValorPorTrecho
			if !tr.Cancelado {
				totalGanhos += ganhoTrecho
			}

			passStr := "nenhum"
			if len(tr.Passageiros) > 0 {
				passStr = strings.Join(tr.Passageiros, ", ")
			}

			fmt.Printf("  • [%s] %s -> %s | Ocupação: %d/%d | Ganho: R$ %.2f\n",
				status, tr.CidadeOrigem, tr.CidadeDestino, tr.AssentosOcupados, c.AssentosDisponiveis, ganhoTrecho)
			fmt.Printf("    Passageiros confirmados: %s\n", passStr)
		}
		fmt.Printf("FATURAMENTO ESTIMADO: R$ %.2f\n", totalGanhos)
		fmt.Println("========================================")
	}
}

func cancelarCarona(conn net.Conn, in, out *bufio.Reader, motoristaEmail string) {
	fmt.Print("Digite o ID da Carona para cancelar: ")
	caronaID, _ := in.ReadString('\n')

	payload, _ := json.Marshal(map[string]string{
		"caronaid":    strings.TrimSpace(caronaID),
		"motoristaid": motoristaEmail,
	})
	req, _ := json.Marshal(Request{Op: "CANCELAR", Data: payload})
	conn.Write(append(req, '\n'))

	resStr, err := out.ReadString('\n')
	if err != nil {
		fmt.Println("Erro de rede.")
		return
	}

	var res Response
	json.Unmarshal([]byte(resStr), &res)
	if res.Ok {
		fmt.Println("Carona cancelada com sucesso.")
	} else {
		fmt.Printf("Falha: %s\n", res.Msg)
	}
}

func cancelarTrecho(conn net.Conn, in, out *bufio.Reader, motoristaEmail string) {
	fmt.Print("ID da Carona (ex: C1): ")
	id, _ := in.ReadString('\n')
	fmt.Print("Cidade Origem do Trecho: ")
	origem, _ := in.ReadString('\n')
	fmt.Print("Cidade Destino do Trecho: ")
	destino, _ := in.ReadString('\n')

	payload, _ := json.Marshal(map[string]string{
		"caronaid":    strings.TrimSpace(id),
		"motoristaid": motoristaEmail,
		"origem":      strings.TrimSpace(origem),
		"destino":     strings.TrimSpace(destino),
	})
	req, _ := json.Marshal(Request{Op: "CANCELARTRECHO", Data: payload})
	conn.Write(append(req, '\n'))

	resStr, _ := out.ReadString('\n')
	var res Response
	json.Unmarshal([]byte(resStr), &res)
	fmt.Println(res.Msg)
}
