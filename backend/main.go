package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	qrcode "github.com/skip2/go-qrcode"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"github.com/neoncorp/pewpaw/backend/database"
	"github.com/neoncorp/pewpaw/backend/socket"
	"github.com/neoncorp/pewpaw/backend/whatsapp"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("PewPaw backend starting...")

	db, err := database.InitAll()
	if err != nil {
		log.Fatalf("database init: %v", err)
	}
	defer db.Close()

	wa := whatsapp.NewClient(database.DataDir)
	if err := wa.Init(context.Background()); err != nil {
		log.Fatalf("whatsmeow init: %v", err)
	}

	ipc := socket.NewListener()
	registerHandlers(ipc, wa, db)

	if err := ipc.Start(); err != nil {
		log.Fatalf("socket listen: %v", err)
	}
	defer ipc.Stop()

	log.Printf("Listening on %s", socket.ListenAddr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go handleWhatsAppEvents(ctx, ipc, wa)
	go handleSignals(cancel, wa)

	if wa.HasDevice() {
		if err := wa.Connect(ctx); err != nil {
			log.Printf("reconnect: %v", err)
		}
	}

	<-ctx.Done()
	log.Println("PewPaw backend shutting down.")
}

func registerHandlers(ipc *socket.Listener, wa *whatsapp.Client, db *database.Manager) {
	ipc.Handle("check_login", func(conn net.Conn, cmd socket.Command) {
		if wa.HasDevice() {
			socket.SendEvent(conn, socket.Event{Type: "login_ok", Payload: nil})
		} else {
			socket.SendEvent(conn, socket.Event{Type: "require_login", Payload: nil})
		}
	})

	ipc.Handle("login", func(conn net.Conn, cmd socket.Command) {
		if wa.HasDevice() {
			socket.SendEvent(conn, socket.Event{Type: "login_ok", Payload: nil})
			return
		}
		qrChan, err := wa.GetQRChannel(context.Background())
		if err != nil {
			socket.SendEvent(conn, socket.Event{Type: "error", Payload: map[string]string{"message": err.Error()}})
			return
		}
		if err := wa.Connect(context.Background()); err != nil {
			socket.SendEvent(conn, socket.Event{Type: "error", Payload: map[string]string{"message": err.Error()}})
			return
		}
		go func() {
			for item := range qrChan {
				if item.Event == "success" {
					socket.SendEvent(conn, socket.Event{Type: "login_ok", Payload: nil})
					return
				} else if item.Event == "code" {
					png, err := qrcode.Encode(item.Code, qrcode.Medium, 256)
					if err == nil {
						b64 := base64.StdEncoding.EncodeToString(png)
						socket.SendEvent(conn, socket.Event{Type: "qr", Payload: map[string]string{"code": "data:image/png;base64," + b64}})
					}
				}
			}
		}()
	})

	ipc.Handle("logout", func(conn net.Conn, cmd socket.Command) {
		if err := wa.Logout(context.Background()); err != nil {
			socket.SendEvent(conn, socket.Event{Type: "error", Payload: map[string]string{"message": err.Error()}})
			return
		}
		socket.SendEvent(conn, socket.Event{Type: "logged_out", Payload: nil})
	})

	ipc.Handle("send_message", func(conn net.Conn, cmd socket.Command) {
		var payload struct {
			JID              string `json:"jid"`
			Message          string `json:"message"`
			ReplyTo          string `json:"reply_to,omitempty"`
			ReplyParticipant string `json:"reply_participant,omitempty"`
			ReplyText        string `json:"reply_text,omitempty"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			socket.SendEvent(conn, socket.Event{Type: "error", Payload: map[string]string{"message": "bad payload"}})
			return
		}
		jid, err := types.ParseJID(payload.JID)
		if err != nil {
			socket.SendEvent(conn, socket.Event{Type: "error", Payload: map[string]string{"message": "invalid jid"}})
			return
		}
		msg := &waE2E.Message{
			Conversation: &payload.Message,
		}

		if payload.ReplyTo != "" {
			msg = &waE2E.Message{
				ExtendedTextMessage: &waE2E.ExtendedTextMessage{
					Text: &payload.Message,
					ContextInfo: &waE2E.ContextInfo{
						StanzaID:      &payload.ReplyTo,
						Participant:   &payload.ReplyParticipant,
						QuotedMessage: &waE2E.Message{Conversation: &payload.ReplyText},
					},
				},
			}
		}

		resp, err := wa.WaCli.SendMessage(context.Background(), jid, msg)
		if err != nil {
			socket.SendEvent(conn, socket.Event{Type: "error", Payload: map[string]string{"message": err.Error()}})
			return
		}
		socket.SendEvent(conn, socket.Event{Type: "message_sent", Payload: resp})
	})

	ipc.Handle("get_chats", func(conn net.Conn, cmd socket.Command) {
		socket.SendEvent(conn, socket.Event{Type: "chats", Payload: []interface{}{}})
	})

	ipc.Handle("chat_presence", func(conn net.Conn, cmd socket.Command) {
		var payload struct {
			JID   string `json:"jid"`
			State string `json:"state"` // "composing", "paused"
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
			if jid, err := types.ParseJID(payload.JID); err == nil {
				state := types.ChatPresenceComposing
				if payload.State == "paused" {
					state = types.ChatPresencePaused
				}
				wa.WaCli.SendChatPresence(context.Background(), jid, state, types.ChatPresenceMediaText)
			}
		}
	})

	ipc.Handle("leave_group", func(conn net.Conn, cmd socket.Command) {
		var payload struct {
			GroupJID string `json:"group_jid"`
		}
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			socket.SendEvent(conn, socket.Event{Type: "error", Payload: map[string]string{"message": "bad payload"}})
			return
		}
		jid, err := types.ParseJID(payload.GroupJID)
		if err != nil {
			socket.SendEvent(conn, socket.Event{Type: "error", Payload: map[string]string{"message": "invalid jid"}})
			return
		}
		err = wa.WaCli.LeaveGroup(context.Background(), jid)
		if err != nil {
			socket.SendEvent(conn, socket.Event{Type: "error", Payload: map[string]string{"message": err.Error()}})
			return
		}
		socket.SendEvent(conn, socket.Event{Type: "group_left", Payload: map[string]string{"jid": payload.GroupJID}})
	})
}

func handleWhatsAppEvents(ctx context.Context, ipc *socket.Listener, wa *whatsapp.Client) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt := <-wa.EventChan():
			switch v := evt.(type) {
			case *events.Message:
				ipc.Broadcast(socket.Event{
					Type: "message",
					Payload: map[string]interface{}{
						"id":         v.Info.ID,
						"chat_jid":   v.Info.Chat.String(),
						"sender":     v.Info.Sender.String(),
						"timestamp":  v.Info.Timestamp.Unix(),
						"is_from_me": v.Info.IsFromMe,
						"text":       extractText(v),
					},
				})
			case *events.ChatPresence:
				ipc.Broadcast(socket.Event{
					Type: "chat_presence",
					Payload: map[string]interface{}{
						"chat_jid": v.MessageSource.Chat.String(),
						"sender":   v.MessageSource.Sender.String(),
						"state":    string(v.State),
					},
				})
			case *events.PairSuccess:
				log.Printf("Pair success: %s", v.ID)
				ipc.Broadcast(socket.Event{Type: "login_ok", Payload: map[string]string{"jid": v.ID.String()}})
			case *events.LoggedOut:
				ipc.Broadcast(socket.Event{Type: "logged_out", Payload: nil})
			case *events.Connected:
				ipc.Broadcast(socket.Event{Type: "connected", Payload: nil})
			case *events.Disconnected:
				ipc.Broadcast(socket.Event{Type: "disconnected", Payload: nil})
			case *events.QR:
				for _, code := range v.Codes {
					png, err := qrcode.Encode(code, qrcode.Medium, 256)
					if err == nil {
						b64 := base64.StdEncoding.EncodeToString(png)
						ipc.Broadcast(socket.Event{Type: "qr", Payload: map[string]string{"code": "data:image/png;base64," + b64}})
					}
				}
			}
		}
	}
}

func extractText(v *events.Message) string {
	if v.Message.GetConversation() != "" {
		return v.Message.GetConversation()
	}
	if v.Message.GetExtendedTextMessage() != nil {
		return v.Message.GetExtendedTextMessage().GetText()
	}
	return ""
}

func handleSignals(cancel context.CancelFunc, wa *whatsapp.Client) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println()
	log.Println("Shutdown signal received.")
	wa.Disconnect()
	cancel()
}
