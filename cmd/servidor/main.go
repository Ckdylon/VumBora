package main

import (
	"log"
	"vumbora/internal/roteamento"
	"vumbora/internal/tcp"
)

func main() {
	//inicializa a memoria e o sistema das reservas
	manager := roteamento.NovoGerenciador()
	//instacia o servidor TCP
	srv := tcp.NovoServidor(":8080", manager)

	log.Println("iniciando servidor na porta 8080")

	//trava a thread principal
	if err := srv.Iniciar(); err != nil {
		log.Fatalf("erro fatal no servidor: %v", err)
	}
}
