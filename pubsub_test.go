package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPublishDeliversToMatchingTopicOnly(t *testing.T) {
	broker := NewBroker()

	goSub, _ := broker.Subscribe("go-room", "sub-go", "aman", 8)
	defer broker.Unsubscribe("go-room", "sub-go")

	rustSub, _ := broker.Subscribe("rust-room", "sub-rust", "alex", 8)
	defer broker.Unsubscribe("rust-room", "sub-rust")

	select {
	case joinMsg := <-goSub.Ch:
		if joinMsg.Kind != "system" {
			t.Fatalf("expected initial system join message on go-room, got %+v", joinMsg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for join message on go-room")
	}

	select {
	case joinMsg := <-rustSub.Ch:
		if joinMsg.Kind != "system" {
			t.Fatalf("expected initial system join message on rust-room, got %+v", joinMsg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for join message on rust-room")
	}

	broker.Publish(Message{
		Kind:   "chat",
		Topic:  "go-room",
		Sender: "aman",
		Body:   "hello go",
	})

	select {
	case msg := <-goSub.Ch:
		if msg.Body != "hello go" {
			t.Fatalf("expected body %q, got %q", "hello go", msg.Body)
		}
		if msg.Sender != "aman" {
			t.Fatalf("expected sender %q, got %q", "aman", msg.Sender)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message on go-room")
	}

	select {
	case msg := <-rustSub.Ch:
		t.Fatalf("did not expect message on rust-room, got %+v", msg)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestHistoryBoundsAndDoesNotStoreSystemEvents(t *testing.T) {
	broker := NewBroker()

	sub, _ := broker.Subscribe("general", "sub-1", "aman", 32)
	defer broker.Unsubscribe("general", "sub-1")

	totalPublished := defaultHistoryLimit + 5
	for i := 1; i <= totalPublished; i++ {
		broker.Publish(Message{
			Kind:   "chat",
			Topic:  "general",
			Sender: "aman",
			Body:   fmt.Sprintf("msg-%d", i),
		})
	}

	hist := broker.History("general")
	if len(hist) != defaultHistoryLimit {
		t.Fatalf("expected history length %d, got %d", defaultHistoryLimit, len(hist))
	}

	wantFirst := fmt.Sprintf("msg-%d", totalPublished-defaultHistoryLimit+1)
	wantLast := fmt.Sprintf("msg-%d", totalPublished)

	if hist[0].Body != wantFirst {
		t.Fatalf("expected oldest retained message %q, got %q", wantFirst, hist[0].Body)
	}
	if hist[len(hist)-1].Body != wantLast {
		t.Fatalf("expected newest retained message %q, got %q", wantLast, hist[len(hist)-1].Body)
	}

	hist[0].Body = "mutated"
	if broker.History("general")[0].Body != wantFirst {
		t.Fatal("expected Broker history to be isolated from caller slice mutations")
	}

	for len(sub.Ch) > 0 {
		<-sub.Ch
	}
}

func TestMemberCountOnSubscribeAndUnsubscribe(t *testing.T) {
	broker := NewBroker()

	_, count1 := broker.Subscribe("general", "s1", "aman", 8)
	if count1 != 1 || broker.MemberCount("general") != 1 {
		t.Fatalf("expected 1 member, got count1=%d, broker=%d", count1, broker.MemberCount("general"))
	}

	_, count2 := broker.Subscribe("general", "s2", "manan", 8)
	if count2 != 2 || broker.MemberCount("general") != 2 {
		t.Fatalf("expected 2 members, got count2=%d, broker=%d", count2, broker.MemberCount("general"))
	}

	broker.Unsubscribe("general", "s1")
	if got := broker.MemberCount("general"); got != 1 {
		t.Fatalf("expected 1 member after unsubscribe, got %d", got)
	}

	broker.Unsubscribe("general", "s2")
	if got := broker.MemberCount("general"); got != 0 {
		t.Fatalf("expected 0 members after last unsubscribe, got %d", got)
	}
}

func TestConcurrentPublishSubscribeUnsubscribeRace(t *testing.T) {
	broker := NewBroker()
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id := fmt.Sprintf("client-%d", idx)
			user := fmt.Sprintf("user-%d", idx)

			sub, _ := broker.Subscribe("lobby", id, user, 64)
			doneRead := make(chan struct{})
			go func() {
				defer close(doneRead)
				for range sub.Ch {
				}
			}()

			time.Sleep(15 * time.Millisecond)
			broker.Unsubscribe("lobby", id)
			<-doneRead
		}(i)
	}

	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func(pubIdx int) {
			defer wg.Done()
			for m := 0; m < 25; m++ {
				broker.Publish(Message{
					Kind:   "chat",
					Topic:  "lobby",
					Sender: fmt.Sprintf("pub-%d", pubIdx),
					Body:   fmt.Sprintf("hello-%d", m),
				})
			}
		}(p)
	}

	wg.Wait()
}