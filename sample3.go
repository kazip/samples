package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"github.com/rs/cors"
	"github.com/zishang520/socket.io/v2/socket"
)

type authData struct {
	Id     int    `json:"id"`
	Email  string `json:"email"`
	Status int    `json:"status"`
}

func init() {
	if err := godotenv.Load(); err != nil {
		fmt.Println("No .env file found")
	}
}

func newRedisClient(redisUrl string) *redis.Client {

	opts, err := redis.ParseURL(redisUrl)
	if err != nil {
		panic(err)
	}

	return redis.NewClient(opts)
}

var subClient *redis.Client

func subscribeRetransmit(redisChan string, socketEvent string, socket *socket.Socket, ctx context.Context, asJson bool) {
	sub := subClient.Subscribe(ctx, redisChan)
	for {
		msg, err := sub.ReceiveMessage(ctx)
		select {
		case <-ctx.Done():
			return
		default:
			if err != nil {
				fmt.Println("Error", redisChan, ":", err)
				continue
			}

			fmt.Println("Message", redisChan, ":", msg)
			message := msg.Payload
			if asJson {
				var v map[string]any
				json.Unmarshal([]byte(message), &v)
				socket.Emit(socketEvent, v)
			} else {
				socket.Emit(socketEvent, message)
			}

		}
	}
}

func subscribeUser(userId int, socket *socket.Socket, ctx context.Context) {
	go subscribeRetransmit(fmt.Sprintf("botChannel-%d", userId), "bot", socket, ctx, true)
	go subscribeRetransmit(fmt.Sprintf("backtestChannel-%d", userId), "backtest", socket, ctx, true)
}

func subscribeAnonym(socket *socket.Socket, ctx context.Context) {
	go subscribeRetransmit("messageChannel", "hello", socket, ctx, false)
}

func main() {

	redisUrl, ok := os.LookupEnv("REDIS_URL")
	if !ok {
		panic("REDIS_URL environment is not set!")
	}

	checkAuthUrl, ok := os.LookupEnv("CHECK_AUTH_URL")
	if !ok {
		panic("CHECK_AUTH_URL environment is not set!")
	}

	subClient = newRedisClient(redisUrl)

	socketIo := socket.NewServer(nil, nil)
	handler := cors.Default().Handler(socketIo.ServeHandler(nil))
	http.Handle("/socket.io/", handler)
	go http.ListenAndServe(":23000", nil)

	socketIo.On("connection", func(clients ...any) {
		ctx, clientCtxCancel := context.WithCancel(context.Background())

		client := clients[0].(*socket.Socket)
		fmt.Printf("Connected, %s\n", client.Id())

		client.On("auth", func(datas ...any) {
			data := datas[0]
			authCtx, authCtxCancel := context.WithCancel(ctx)

			if auth, ok := data.(map[string]any); ok {

				if token, ok := auth["token"]; ok {
					fmt.Printf("Auth with token %T\n", token)
					if tokenString, ok := token.(string); ok {
						httpClient := &http.Client{}
						req, err := http.NewRequest("GET", checkAuthUrl, nil)
						if err != nil {
							fmt.Println(err)
							return
						}

						req.Header.Add("Authorization", "Bearer "+tokenString)
						resp, err := httpClient.Do(req)
						if err != nil {
							fmt.Println(err)
							return
						}
						defer resp.Body.Close()

						bodyBytes, err := io.ReadAll(resp.Body)
						if err != nil {
							fmt.Println(err)
							return
						}
						bodyString := string(bodyBytes)
						fmt.Println(bodyString)

						var userData authData
						err = json.Unmarshal(bodyBytes, &userData)
						if err != nil {
							fmt.Println(err)
						}

						subscribeUser(userData.Id, client, authCtx)

						client.On("logout", func(a ...any) {
							authCtxCancel()
						})

					}

				}
			}
		})

		subscribeAnonym(client, ctx)

		client.On("disconnect", func(a ...any) {
			clientCtxCancel()
		})
	})

	exit := make(chan struct{})
	SignalC := make(chan os.Signal)

	signal.Notify(SignalC, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for s := range SignalC {
			switch s {
			case os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				close(exit)
				return
			}
		}
	}()

	<-exit
	socketIo.Close(nil)
	os.Exit(0)

}
