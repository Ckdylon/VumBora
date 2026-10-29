package tcp

import (
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"vumbora/internal/dominio"
	"vumbora/internal/roteamento"
)

func TestConsorcioSobCargaEInjecaoDeFalhas(t *testing.T) {
	dirTemp := t.TempDir()
	arqA := filepath.Join(dirTemp, "emp_a.json")
	arqB := filepath.Join(dirTemp, "emp_b.json")
	arqC := filepath.Join(dirTemp, "emp_c.json")

	peersA := make(map[string]string)
	peersB := make(map[string]string)
	peersC := make(map[string]string)

	mA := roteamento.NovoGerenciadorConsorcio("EMP_A", arqA, peersA)
	mB := roteamento.NovoGerenciadorConsorcio("EMP_B", arqB, peersB)
	mC := roteamento.NovoGerenciadorConsorcio("EMP_C", arqC, peersC)

	srvA := httptest.NewServer(NovoServidor(":0", mA).NovoMuxREST())
	defer srvA.Close()
	srvB := httptest.NewServer(NovoServidor(":0", mB).NovoMuxREST())
	defer srvB.Close()
	srvC := httptest.NewServer(NovoServidor(":0", mC).NovoMuxREST())

	peersA["EMP_B"], peersA["EMP_C"] = srvB.URL, srvC.URL
	peersB["EMP_A"], peersB["EMP_C"] = srvA.URL, srvC.URL
	peersC["EMP_A"], peersC["EMP_B"] = srvA.URL, srvB.URL

	dataViagem := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	const capacidadeAssentos = 2

	// Publica 1 carona em cada empresa compondo Salvador -> Feira -> Conquista -> Montes Claros
	idA := mA.PublicarCarona(dominio.Carona{
		MotoristaID:         "motA",
		DataPartida:         dataViagem,
		AssentosDisponiveis: capacidadeAssentos,
		ValorPorTrecho:      30,
		Trechos:             []dominio.TrechoCarona{{CidadeOrigem: "Salvador", CidadeDestino: "Feira"}},
	})
	idB := mB.PublicarCarona(dominio.Carona{
		MotoristaID:         "motB",
		DataPartida:         dataViagem,
		AssentosDisponiveis: capacidadeAssentos,
		ValorPorTrecho:      50,
		Trechos:             []dominio.TrechoCarona{{CidadeOrigem: "Feira", CidadeDestino: "Conquista"}},
	})
	idC := mC.PublicarCarona(dominio.Carona{
		MotoristaID:         "motC",
		DataPartida:         dataViagem,
		AssentosDisponiveis: capacidadeAssentos,
		ValorPorTrecho:      70,
		Trechos:             []dominio.TrechoCarona{{CidadeOrigem: "Conquista", CidadeDestino: "Montes Claros"}},
	})

	itinerarios := mA.BuscarItinerarios("Salvador", "Montes Claros", dataViagem)
	if len(itinerarios) != 1 {
		t.Fatalf("Esperava 1 itinerario federado de 3 empresas, obteve %d", len(itinerarios))
	}
	itinID := itinerarios[0].ItinerarioID
	t.Logf("Itinerario federado montado: %s (Valor: R$ %.2f)", itinID, itinerarios[0].ValorTotal)

	// --- CENARIO 1: 30 passageiros simultaneos em 3 servidores disputando 2 vagas ---
	const totalConcorrentes = 30
	managers := []*roteamento.GerenciadorCaronas{mA, mB, mC}
	var confirmadas int32
	var wg sync.WaitGroup
	largada := make(chan struct{})

	for i := 0; i < totalConcorrentes; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			coordenador := managers[idx%len(managers)]
			passageiro := fmt.Sprintf("pass_%02d@teste.com", idx)
			<-largada
			if coordenador.Reservar(itinID, "Salvador", "Montes Claros", passageiro) {
				atomic.AddInt32(&confirmadas, 1)
			}
		}(i)
	}
	close(largada)
	wg.Wait()

	if confirmadas != capacidadeAssentos {
		t.Fatalf("Violacao de atomicidade/exclusao mutua: esperava %d reservas confirmadas, obteve %d",
			capacidadeAssentos, confirmadas)
	}
	t.Logf("[OK] Cenario 1: Exatamente %d/%d reservas confirmadas sem overbooking entre 3 empresas.", confirmadas, totalConcorrentes)

	// --- CENARIO 2: Injecao de Falha (Queda da EMP_C) + Verificacao de Rollback e TTL ---
	// Publica nova rota com 1 vaga em cada empresa
	idA2 := mA.PublicarCarona(dominio.Carona{
		MotoristaID:         "motA2",
		DataPartida:         dataViagem,
		AssentosDisponiveis: 1,
		ValorPorTrecho:      25,
		Trechos:             []dominio.TrechoCarona{{CidadeOrigem: "Salvador", CidadeDestino: "Alagoinhas"}},
	})
	idB2 := mB.PublicarCarona(dominio.Carona{
		MotoristaID:         "motB2",
		DataPartida:         dataViagem,
		AssentosDisponiveis: 1,
		ValorPorTrecho:      35,
		Trechos:             []dominio.TrechoCarona{{CidadeOrigem: "Alagoinhas", CidadeDestino: "Aracaju"}},
	})
	idC2 := mC.PublicarCarona(dominio.Carona{
		MotoristaID:         "motC2",
		DataPartida:         dataViagem,
		AssentosDisponiveis: 1,
		ValorPorTrecho:      45,
		Trechos:             []dominio.TrechoCarona{{CidadeOrigem: "Aracaju", CidadeDestino: "Maceio"}},
	})

	itinFalha := fmt.Sprintf("%s@Salvador>Alagoinhas+%s@Alagoinhas>Aracaju+%s@Aracaju>Maceio", idA2, idB2, idC2)

	// Simula queda abrupta de um coordenador apos segurar PreReserva na EMP_A (teste de expiracao TTL)
	prepOrfa := mA.PreReservarLocal(dominio.RestPrepareRequest{
		TxID:         "TX-COORD-MORTO-999",
		CaronaID:     idA2,
		Origem:       "Salvador",
		Destino:      "Alagoinhas",
		PassageiroID: "fantasma@teste.com",
		EmpresaCoord: "EMP_X",
		LamportTS:    500,
	})
	if !prepOrfa.Ok {
		t.Fatalf("Falha ao criar pre-reserva para teste de TTL")
	}

	// Derruba o servidor da Empresa C!
	srvC.Close()
	t.Log("[FALHA INJETADA] Servidor da EMP_C interrompido.")

	// Tenta reservar o itinerario que depende da EMP_C: deve falhar e fazer rollback imediato de A e B
	if mA.Reservar(itinFalha, "Salvador", "Maceio", "viajante_falha@teste.com") {
		t.Fatalf("Erro grave: Itinerario foi confirmado mesmo com o servidor da EMP_C desligado!")
	}

	// Verifica que os trechos de A e B NAO ficaram reservados pela metade
	caronasA := mA.ListarPorMotorista("motA2")
	caronasB := mB.ListarPorMotorista("motB2")
	if caronasA[0].Trechos[0].AssentosOcupados != 0 || caronasB[0].Trechos[0].AssentosOcupados != 0 {
		t.Fatalf("Erro grave: Itinerario reservado pela metade apos falha da EMP_C!")
	}
	t.Log("[OK] Cenario 2a: Nenhum itinerario foi reservado pela metade com EMP_C offline.")

	// Aguarda expirar o TTL (3s) da pre-reserva orfa e verifica que A e B operam normalmente entre si
	time.Sleep(3200 * time.Millisecond)
	itinAB := fmt.Sprintf("%s@Salvador>Alagoinhas+%s@Alagoinhas>Aracaju", idA2, idB2)
	if !mA.Reservar(itinAB, "Salvador", "Aracaju", "passageiro_ab@teste.com") {
		t.Fatalf("Erro grave: Assento permaneceu bloqueado apos expiracao do TTL ou falha de C afetou rotas A+B!")
	}
	t.Log("[OK] Cenario 2b: Bloqueio orfao expirou por TTL e reserva entre EMP_A e EMP_B funcionou com EMP_C desligada.")

	// --- CENARIO 3: Recuperacao do Servidor da EMP_C a partir do Disco ---
	mCRecuperado := roteamento.NovoGerenciadorConsorcio("EMP_C", arqC, peersC)
	srvCRecuperado := httptest.NewServer(NovoServidor(":0", mCRecuperado).NovoMuxREST())
	defer srvCRecuperado.Close()
	peersA["EMP_C"] = srvCRecuperado.URL
	peersB["EMP_C"] = srvCRecuperado.URL

	caronasCRec := mCRecuperado.ListarPorMotorista("motC")
	if len(caronasCRec) != 1 || caronasCRec[0].Trechos[0].AssentosOcupados != capacidadeAssentos {
		t.Fatalf("Inconsistencia pos-recuperacao na EMP_C (%s): esperava %d assentos ocupados na carona %s/%s",
			idC, capacidadeAssentos, idA, idB)
	}
	if len(mCRecuperado.ListarPorMotorista("motC2")) != 1 || mCRecuperado.ListarPorMotorista("motC2")[0].Trechos[0].AssentosOcupados != 0 {
		t.Fatalf("Inconsistencia pos-recuperacao na carona %s", idC2)
	}
	t.Log("[OK] Cenario 3: Servidor EMP_C recuperou estado consistente do disco apos reinicializacao.")
}
