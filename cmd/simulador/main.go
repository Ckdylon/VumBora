package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type reqTCP struct {
	Op   string      `json:"op"`
	Data interface{} `json:"data"`
}

type resTCP struct {
	Ok  bool            `json:"ok"`
	Msg string          `json:"msg"`
	Res json.RawMessage `json:"res,omitempty"`
}

func enviarComando(addr string, comando reqTCP) (resTCP, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return resTCP{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(4 * time.Second))

	b, _ := json.Marshal(comando)
	if _, err := conn.Write(append(b, '\n')); err != nil {
		return resTCP{}, err
	}

	linha, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return resTCP{}, err
	}

	var resp resTCP
	err = json.Unmarshal([]byte(linha), &resp)
	return resp, err
}

func main() {
	// Enderecos TCP dos 3 servidores do consorcio (parametrizavel via env SERVERS_TCP)
	servidores := []string{"localhost:8080", "localhost:8090", "localhost:8100"}
	if envSrv := os.Getenv("SERVERS_TCP"); strings.TrimSpace(envSrv) != "" {
		servidores = strings.Split(envSrv, ",")
	}

	fmt.Println("=== SIMULADOR AUTONOMO DE CARGA DO CONSORCIO VUMBORA ===")
	fmt.Printf("Servidores alvo (TCP): %v\n\n", servidores)

	dataViagem := "2026-10-30"
	vagasPorTrecho := 5

	// Publica caronas em cada uma das 3 empresas formando a rota Salvador -> Montes Claros
	rotasIniciais := []struct {
		srvIdx   int
		mot      string
		rota     []string
		preco    float64
		assentos int
	}{
		{0, "mot_a@vumbora.com", []string{"Salvador", "Feira"}, 30.0, vagasPorTrecho},
		{1, "mot_b@vaijunto.com", []string{"Feira", "Conquista"}, 60.0, vagasPorTrecho},
		{2, "mot_c@bahiacar.com", []string{"Conquista", "Montes Claros"}, 80.0, vagasPorTrecho},
	}

	for _, r := range rotasIniciais {
		if r.srvIdx >= len(servidores) {
			continue
		}
		resp, err := enviarComando(servidores[r.srvIdx], reqTCP{
			Op: "PUBLICAR",
			Data: map[string]interface{}{
				"motoristaid": r.mot,
				"data":        dataViagem,
				"preco":       r.preco,
				"assentos":    r.assentos,
				"rota":        r.rota,
			},
		})
		if err != nil {
			fmt.Printf("[AVISO] Falha ao publicar no servidor %s: %v\n", servidores[r.srvIdx], err)
		} else {
			fmt.Printf("[SETUP] Servidor %s -> %s\n", servidores[r.srvIdx], resp.Msg)
		}
	}

	// Dispara rajada concorrente de passageiros distribuidos entre todas as empresas
	totalPassageiros := 30
	var wg sync.WaitGroup
	var buscasOk, reservasConfirmadas, reservasRecusadas int32
	barreira := make(chan struct{})

	fmt.Printf("\nDisparando %d passageiros simultaneos em %d servidores para %d vagas federadas...\n",
		totalPassageiros, len(servidores), vagasPorTrecho)

	inicio := time.Now()
	for i := 1; i <= totalPassageiros; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			srvAlvo := servidores[id%len(servidores)]
			email := fmt.Sprintf("passageiro_%02d@teste.com", id)

			<-barreira // Aguarda liberacao simultanea

			// Busca itinerario federado Salvador -> Montes Claros
			resBusca, err := enviarComando(srvAlvo, reqTCP{
				Op: "BUSCAR",
				Data: map[string]string{
					"origem":  "Salvador",
					"destino": "Montes Claros",
					"data":    dataViagem,
				},
			})
			if err != nil || !resBusca.Ok {
				atomic.AddInt32(&reservasRecusadas, 1)
				return
			}

			var itinerarios []struct {
				ItinerarioID string `json:"itinerarioid"`
			}
			_ = json.Unmarshal(resBusca.Res, &itinerarios)
			if len(itinerarios) == 0 {
				atomic.AddInt32(&reservasRecusadas, 1)
				return
			}
			atomic.AddInt32(&buscasOk, 1)

			// Tenta reservar o itinerario composto de 3 empresas via 2PC
			resReserva, err := enviarComando(srvAlvo, reqTCP{
				Op: "RESERVAR",
				Data: map[string]string{
					"caronaid":     itinerarios[0].ItinerarioID,
					"origem":       "Salvador",
					"destino":      "Montes Claros",
					"passageiroid": email,
				},
			})
			if err == nil && resReserva.Ok {
				atomic.AddInt32(&reservasConfirmadas, 1)
				fmt.Printf("  [OK] %s (via %s) confirmou itinerario %s\n", email, srvAlvo, itinerarios[0].ItinerarioID)
			} else {
				atomic.AddInt32(&reservasRecusadas, 1)
			}
		}(i)
	}

	close(barreira)
	wg.Wait()
	duracao := time.Since(inicio)

	fmt.Println("\n--- RESULTADO DA SIMULACAO DE CARGA ---")
	fmt.Printf("Tempo total de execucao : %v\n", duracao)
	fmt.Printf("Buscas federadas c/ rota: %d\n", buscasOk)
	fmt.Printf("Reservas Confirmadas    : %d (Limite fisico: %d)\n", reservasConfirmadas, vagasPorTrecho)
	fmt.Printf("Reservas Recusadas (409): %d\n", reservasRecusadas)
}
