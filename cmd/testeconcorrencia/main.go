package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Request struct {
	Op   string          `json:"op"`
	Data json.RawMessage `json:"data"`
}

type Response struct {
	Ok  bool            `json:"ok"`
	Msg string          `json:"msg"`
	Res json.RawMessage `json:"res,omitempty"`
}

const (
	TotalConcorrentes = 20
	CidadeOrigem      = "Feira"
	CidadeDestino     = "Salvador"
	DataViagem        = "2026-11-20"
)

func main() {
	addr := os.Getenv("SERVER_ADDR")
	if addr == "" {
		addr = "localhost:8080"
	}

	fmt.Println("=====================================================")
	fmt.Println("   VUMBORA - Teste automatizado de concorrência")
	fmt.Printf("   Alvo: %s | Disputa: %d goroutines para 1 vaga\n", addr, TotalConcorrentes)
	fmt.Println("=====================================================")

	//Motorista publica uma carona com APENAS 1 assento
	caronaID := prepararCenario(addr)
	if caronaID == "" {
		fmt.Println("Falha ao inicializar o cenário de teste.")
		return
	}
	fmt.Printf("Carona [%s] criada com 1 assento disponível.\n", caronaID)
	fmt.Printf("Preparando %d conexões de passageiros concorrentes...\n\n", TotalConcorrentes)

	var (
		wgProntos    sync.WaitGroup // Garante que todos conectaram antes do disparo
		wgFinal      sync.WaitGroup // Aguarda todas as respostas
		startSinal   = make(chan struct{})
		sucessos     int32
		falhas       int32
		totalTempoNs int64
	)

	wgProntos.Add(TotalConcorrentes)
	wgFinal.Add(TotalConcorrentes)

	// Criação das 20 Goroutines concorrentes
	for i := 1; i <= TotalConcorrentes; i++ {
		idPassageiro := fmt.Sprintf("passageiro_teste_%02d@teste.com", i)

		go func(pID string, index int) {
			defer wgFinal.Done()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				fmt.Printf("[%s] Erro de conexão: %v\n", pID, err)
				atomic.AddInt32(&falhas, 1)
				wgProntos.Done()
				return
			}
			defer conn.Close()

			in := bufio.NewReader(conn)

			// Autenticação prévia
			loginPayload, _ := json.Marshal(map[string]string{"email": pID, "senha": "123"})
			reqLogin, _ := json.Marshal(Request{Op: "LOGIN", Data: loginPayload})
			conn.Write(append(reqLogin, '\n'))
			in.ReadString('\n')

			// Prepara o payload da reserva
			reservaPayload, _ := json.Marshal(map[string]string{
				"caronaid":     caronaID,
				"origem":       CidadeOrigem,
				"destino":      CidadeDestino,
				"passageiroid": pID,
			})
			reqReserva, _ := json.Marshal(Request{Op: "RESERVAR", Data: reservaPayload})
			msgBytes := append(reqReserva, '\n')

			// Sinaliza que está conectado e aguardando a largada
			wgProntos.Done()

			// Barreira para sicronizar todas as goroutines, disparam no mesmo nanossegundo
			<-startSinal

			tInicio := time.Now()
			conn.Write(msgBytes)

			resStr, err := in.ReadString('\n')
			duracao := time.Since(tInicio)
			atomic.AddInt64(&totalTempoNs, duracao.Nanoseconds())

			if err != nil {
				atomic.AddInt32(&falhas, 1)
				return
			}

			var res Response
			json.Unmarshal([]byte(resStr), &res)

			if res.Ok {
				atomic.AddInt32(&sucessos, 1)
				fmt.Printf("  [Goroutine #%02d] Ficou com a vaga (%s) em %v\n", index, pID, duracao)
			} else {
				atomic.AddInt32(&falhas, 1)
				fmt.Printf("  [Goroutine #%02d] Rejeitado: %s (%v)\n", index, res.Msg, duracao)
			}
		}(idPassageiro, i)
	}

	// Aguarda todos os 20 passageiros conectarem e realizarem login
	wgProntos.Wait()
	fmt.Println("Disparando 20 requisições simultâneas agora...")
	tGlobal := time.Now()

	// Libera todas as goroutines ao mesmo tempo
	close(startSinal)

	// Aguarda término de todas as requisições
	wgFinal.Wait()
	tempoTotalGasto := time.Since(tGlobal)

	// 3. Apresentação e Verificação dos Resultados
	mediaLatencia := time.Duration(totalTempoNs / int64(TotalConcorrentes))

	fmt.Println("\n=====================================================")
	fmt.Println("               RELATÓRIO DE DESEMPENHO               ")
	fmt.Println("=====================================================")
	fmt.Printf("Total de Concorrentes       : %d\n", TotalConcorrentes)
	fmt.Printf("Reservas Aceitas            : %d (Esperado: 1)\n", sucessos)
	fmt.Printf("Reservas Negadas (Limpo)    : %d (Esperado: %d)\n", falhas, TotalConcorrentes-1)
	fmt.Printf("Tempo Total do Experimento  : %v\n", tempoTotalGasto)
	fmt.Printf("Latência Média por Reserva  : %v\n", mediaLatencia)
	fmt.Println("-----------------------------------------------------")

	if sucessos == 1 && falhas == TotalConcorrentes-1 {
		fmt.Println("RESULTADO: TESTE PASSOU COM SUCESSO")
		fmt.Println("Atomicidade da reserva preservada (sem overbooking).")
		fmt.Println("Canais CSP processaram a concorrência sem race conditions.")
	} else {
		fmt.Printf("ERRO DE CONCORRÊNCIA DETECTADO. Vagas cedidas: %d\n", sucessos)
	}
	fmt.Println("=====================================================")
}

// Cria uma carona de teste com 1 vaga e obtém seu ID
func prepararCenario(addr string) string {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("Não foi possível conectar ao servidor em %s: %v\n", addr, err)
		return ""
	}
	defer conn.Close()

	in := bufio.NewReader(conn)

	// Login do motorista
	loginB, _ := json.Marshal(map[string]string{"email": "mot_teste@teste.com", "senha": "123"})
	conn.Write(append([]byte(fmt.Sprintf(`{"op":"LOGIN","data":%s}`, loginB)), '\n'))
	in.ReadString('\n')

	// Publicar carona com 1 assento
	pubPayload, _ := json.Marshal(map[string]interface{}{
		"motorista_id": "mot_teste@teste.com",
		"data":         DataViagem,
		"preco":        30.0,
		"assentos":     1, // Apenas UMA vaga para 20 pessoas disputarem
		"rota":         []string{CidadeOrigem, CidadeDestino},
	})
	conn.Write(append([]byte(fmt.Sprintf(`{"op":"PUBLICAR","data":%s}`, pubPayload)), '\n'))
	in.ReadString('\n')

	// Busca a carona recém-criada para obter o ID exato gerado pelo monitor
	buscaPayload, _ := json.Marshal(map[string]string{
		"origem":  CidadeOrigem,
		"destino": CidadeDestino,
		"data":    DataViagem,
	})
	conn.Write(append([]byte(fmt.Sprintf(`{"op":"BUSCAR","data":%s}`, buscaPayload)), '\n'))
	resStr, _ := in.ReadString('\n')

	var res Response
	json.Unmarshal([]byte(resStr), &res)

	var itinerarios []struct {
		CaronaID string `json:"caronaid"`
	}
	json.Unmarshal(res.Res, &itinerarios)

	if len(itinerarios) > 0 {
		return itinerarios[0].CaronaID
	}
	return ""
}
