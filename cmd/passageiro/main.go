package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"vumbora/internal/roteamento"
)

// replicando as structs do protocolo cliente. As tags ajuda
type MensagemRequisicao struct {
	Op   string          `json:"op"`
	Data json.RawMessage `json:"data"`
}
type MensagemResposta struct {
	Ok  bool            `json:"ok"`
	Msg string          `json:"msg"`
	Res json.RawMessage `json:"res,omitempty"`
}
type PayloadBusca struct {
	Origem  string `json:"origem"`
	Destino string `json:"destino"`
	// depois colocaro time aqui, testar primeiro a outra logica
}

func main() {

	enderecoServidor := os.Getenv("SERVER_ADDR")
	if enderecoServidor == "" {
		enderecoServidor = "localhost:8080" // se estiver vazio permite testar localmente
	}

	conexaoTCP, err := net.Dial("tcp", enderecoServidor)
	if err != nil {
		fmt.Printf("erro na conexao com o servidor %v\n", err)
		return
	}
	defer conexaoTCP.Close()

	fmt.Println("------ VUMBORA - Passageiro -----")

	//usando buffer para entradas do teclado
	leitorTerminal := bufio.NewReader(os.Stdin)
	leitorRede := bufio.NewReader(conexaoTCP)
	emailPassageiro := autenticar(conexaoTCP, leitorTerminal, leitorRede)

	for {
		fmt.Println("\nMENU PASSAGEIRO (" + emailPassageiro + ")")
		fmt.Println("[1] Buscar e Reservar Carona")
		fmt.Println("[2] Minhas Viagens")
		fmt.Println("[3] Cancelar Reserva")
		fmt.Println("[4] Sair")
		fmt.Print("Escolha uma opção: ")

		opcaoMenu, _ := leitorTerminal.ReadString('\n')
		opcaoMenu = strings.TrimSpace(opcaoMenu)

		switch opcaoMenu {
		case "1":
			buscarEReservar(conexaoTCP, leitorTerminal, leitorRede, emailPassageiro)
		case "2":
			listarMinhasViagens(conexaoTCP, leitorRede, emailPassageiro)
		case "3":
			cancelarReserva(conexaoTCP, leitorTerminal, leitorRede, emailPassageiro)
		case "4":
			return
		default:
			fmt.Println("Opção inválida")
		}
	}
}

func autenticar(conexaoTCP net.Conn, leitorTerminal, leitorRede *bufio.Reader) string {
	for {
		fmt.Println("\n--- Autenticação Vumbora ---")
		fmt.Print("Email: ")
		email, _ := leitorTerminal.ReadString('\n')
		email = strings.TrimSpace(email)

		fmt.Print("Senha: ")
		senha, _ := leitorTerminal.ReadString('\n')
		senha = strings.TrimSpace(senha)

		payload, _ := json.Marshal(map[string]string{
			"email": email,
			"senha": senha,
		})
		req, _ := json.Marshal(MensagemRequisicao{Op: "LOGIN", Data: payload})
		conexaoTCP.Write(append(req, '\n'))

		resStr, err := leitorRede.ReadString('\n')
		if err != nil {
			fmt.Println("Erro de rede ao logar.")
			continue
		}

		var res MensagemResposta
		json.Unmarshal([]byte(resStr), &res)
		if res.Ok {
			fmt.Println("Acesso liberado")
			return email
		}
		fmt.Printf(" %s. Tente novamente.\n", res.Msg)
	}
}

func buscarEReservar(conexaoTCP net.Conn, leitorTerminal, leitorRede *bufio.Reader, email string) {
	fmt.Print("Origem: ")
	origem, _ := leitorTerminal.ReadString('\n')
	origem = strings.TrimSpace(origem)

	fmt.Print("Destino: ")
	destino, _ := leitorTerminal.ReadString('\n')
	destino = strings.TrimSpace(destino)

	fmt.Print("Data da Partida (Dia-Mês): ")
	dataRaw, _ := leitorTerminal.ReadString('\n')
	partes := strings.Split(strings.TrimSpace(dataRaw), "-")
	if len(partes) != 2 {
		fmt.Println("Use o formato DD-MM (ex: 15-02)")
		return
	}

	dataFormatada := fmt.Sprintf("2026-%s-%s", partes[1], partes[0])

	//especifico
	payload, _ := json.Marshal(map[string]string{
		"origem":  origem,
		"destino": destino,
		"data":    dataFormatada,
	})
	req, _ := json.Marshal(MensagemRequisicao{Op: "BUSCAR", Data: payload})
	conexaoTCP.Write(append(req, '\n'))

	resStr, err := leitorRede.ReadString('\n')
	if err != nil {
		fmt.Println("Erro de rede na busca")
		return
	}
	var res MensagemResposta
	json.Unmarshal([]byte(resStr), &res)

	if !res.Ok {
		fmt.Printf("Erro %s\n", res.Msg)
		return
	}

	var itinerarios []struct {
		CaronaID string `json:"caronaid"`
		Trechos  []struct {
			Origem  string `json:"cidadeorigem"`
			Destino string `json:"cidadedestino"`
		} `json:"trechosconectados"`
		Valor float64 `json:"valortotal"`
	}
	json.Unmarshal(res.Res, &itinerarios)

	if len(itinerarios) == 0 {
		fmt.Println("Nenhum itinerário encontrado para esta data/rota.")
		return
	}

	fmt.Println("\n Itinerários Encontrados:")
	for i, it := range itinerarios {
		fmt.Printf("[%d] Carona %s | Valor: R$ %.2f\n", i+1, it.CaronaID, it.Valor)
		for _, tr := range it.Trechos {
			fmt.Printf("    Trecho: %s -> %s\n", tr.Origem, tr.Destino)
		}
	}

	// reserva direta
	fmt.Print("\nDigite o número do itinerário para reservar (ou 0 para cancelar): ")
	escolhaStr, _ := leitorTerminal.ReadString('\n')
	escolha, _ := strconv.Atoi(strings.TrimSpace(escolhaStr))

	if escolha >= 1 && escolha <= len(itinerarios) {
		escolhido := itinerarios[escolha-1]
		payloadReserva, _ := json.Marshal(map[string]string{
			"caronaid":     escolhido.CaronaID,
			"origem":       origem,
			"destino":      destino,
			"passageiroid": email,
		})
		reqRes, _ := json.Marshal(MensagemRequisicao{Op: "RESERVAR", Data: payloadReserva})
		conexaoTCP.Write(append(reqRes, '\n'))

		respResStr, _ := leitorRede.ReadString('\n')
		var resFinal MensagemResposta
		json.Unmarshal([]byte(respResStr), &resFinal)
		if resFinal.Ok {
			fmt.Println("Reserva confirmada com sucesso")
		} else {
			fmt.Printf("Falha na reserva: %s\n", resFinal.Msg)
		}
	}
}

func listarMinhasViagens(conn net.Conn, out *bufio.Reader, email string) {
	payload, _ := json.Marshal(map[string]string{"passageiroid": email})
	req, _ := json.Marshal(MensagemRequisicao{Op: "MINHASVIAGENS", Data: payload})
	conn.Write(append(req, '\n'))

	resStr, _ := out.ReadString('\n')
	var res MensagemResposta
	json.Unmarshal([]byte(resStr), &res)

	if !res.Ok {
		fmt.Printf("Erro do servidor: %s\n", res.Msg)
		return
	}

	var viagens []roteamento.MinhaViagem
	json.Unmarshal(res.Res, &viagens)

	if len(viagens) == 0 {
		fmt.Println("Você ainda não tem nenhuma viagem confirmada.")
		return
	}

	fmt.Println("\n--- MINHAS VIAGENS CONFIRMADAS ---")
	for _, v := range viagens {
		fmt.Printf("• Carona %s | Data: %s | %s -> %s | Valor: R$ %.2f\n",
			v.CaronaID, v.Data, v.Origem, v.Destino, v.Valor)
	}
}

// isola e retorna as viagens em fatias para usar no cancelador de rezervas ou em outras funções
func obterViagens(conn net.Conn, out *bufio.Reader, email string) []roteamento.MinhaViagem {
	payload, _ := json.Marshal(map[string]string{
		"passageiroid": email,
	})

	req, _ := json.Marshal(MensagemRequisicao{Op: "MINHASVIAGENS", Data: payload})
	conn.Write(append(req, '\n'))

	resStr, err := out.ReadString('\n')
	if err != nil {
		return nil
	}

	var res MensagemResposta
	json.Unmarshal([]byte(resStr), &res)
	if !res.Ok {
		return nil
	}

	var viagens []roteamento.MinhaViagem
	json.Unmarshal(res.Res, &viagens)
	return viagens
}

func cancelarReserva(conn net.Conn, leitorTerminal, leitorRede *bufio.Reader, email string) {
	viagens := obterViagens(conn, leitorRede, email)
	if len(viagens) == 0 {
		fmt.Println("Você não possui reservas para cancelar.")
		return
	}

	fmt.Println("\nEscolha qual reserva deseja cancelar:")
	for i, v := range viagens {
		fmt.Printf("[%d] Carona %s | %s -> %s | Data: %s\n", i+1, v.CaronaID, v.Origem, v.Destino, v.Data)
	}

	fmt.Print("\nDigite o número da viagem (ou 0 para voltar): ")
	escolhaStr, _ := leitorTerminal.ReadString('\n')
	escolha, _ := strconv.Atoi(strings.TrimSpace(escolhaStr))

	if escolha >= 1 && escolha <= len(viagens) {
		escolhida := viagens[escolha-1]
		payload, _ := json.Marshal(map[string]string{
			"caronaid":     escolhida.CaronaID,
			"origem":       escolhida.Origem,
			"destino":      escolhida.Destino,
			"passageiroid": email,
		})
		req, _ := json.Marshal(MensagemRequisicao{Op: "CANCELARRESERVA", Data: payload})
		conn.Write(append(req, '\n'))

		resStr, err := leitorRede.ReadString('\n')
		if err != nil {
			fmt.Println("Erro de rede ao cancelar.")
			return
		}

		var res MensagemResposta
		json.Unmarshal([]byte(resStr), &res)
		if res.Ok {
			fmt.Println("Reserva cancelada com sucesso! O assento foi liberado.")
		} else {
			fmt.Printf("Problema no cancelamento: %s\n", res.Msg)
		}
	}
}
