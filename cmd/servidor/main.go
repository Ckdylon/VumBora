package main

import (
	"log"
	"os"
	"vumbora/internal/roteamento"
	"vumbora/internal/tcp"
)

func main() {
	tcpAddr := os.Getenv("TCP_ADDR")
	if tcpAddr == "" {
		tcpAddr = ":8080"
	}

	httpAddr := os.Getenv("HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = ":8081"
	}
	//inicializa a memoria e o sistema das reservas
	manager := roteamento.NovoGerenciador()
	//instacia o servidor TCP
	srv := tcp.NovoServidor(tcpAddr, manager)

	//API REST entre servidores

	go func() {
		if err := srv.IniciarREST(httpAddr); err != nil {
			log.Fatalf("erro fatal no servidor REST (%s): %v", httpAddr, err)
		}
	}()

	log.Printf("Iniciando servidor da empresa %s | TCP: %s | REST: %s", manager.EmpresaID(), tcpAddr, httpAddr)

	// Mantem a goroutine principal escutando conexoes TCP dos clientes
	if err := srv.Iniciar(); err != nil {
		log.Fatalf("erro fatal no servidor TCP (%s): %v", tcpAddr, err)
	}
}
