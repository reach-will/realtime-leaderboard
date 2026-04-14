import grpc from "k6/net/grpc";
import { check, sleep } from "k6";

const client = new grpc.Client();
client.load(["../api/leaderboard/v1"], "leaderboard.proto");

export const options = {
  stages: [
    { duration: "15s", target: 10 },  // ramp up to 10 VUs
    { duration: "30s", target: 10 },  // sustained load
    { duration: "15s", target: 0  },  // ramp down
  ],
  thresholds: {
    "grpc_req_duration": ["p(95)<100"],  // 95th percentile under 100ms
    "checks": ["rate>0.99"],             // 99% of checks must pass
  },
};

// Each VU has its own JS context, so this flag is per-VU, not shared.
// Connecting once per VU reuses the TCP connection across iterations.
let connected = false;

export default function () {
  if (!connected) {
    client.connect("localhost:50051", { plaintext: true });
    connected = true;
  }

  // GetTop
  const topRes = client.invoke("leaderboard.v1.LeaderboardService/GetTop", {
    limit: 10,
  });
  check(topRes, {
    "GetTop status OK": (r) => r.status === grpc.StatusOK,
    "GetTop returns players": (r) => r.message.players && r.message.players.length > 0,
  });

  // GetPlayer — pick a random player from the live top-10 results.
  // Using real IDs from Redis avoids the previous bug where hardcoded names
  // ("alice", "bob", ...) never existed, causing every check to pass as NOT_FOUND.
  const players = topRes.message && topRes.message.players;
  if (players && players.length > 0) {
    const player = players[Math.floor(Math.random() * players.length)];
    const playerRes = client.invoke(
      "leaderboard.v1.LeaderboardService/GetPlayer",
      { player_id: player.player_id }
    );
    check(playerRes, {
      "GetPlayer status OK": (r) => r.status === grpc.StatusOK,
    });
  }

  sleep(1);
}
