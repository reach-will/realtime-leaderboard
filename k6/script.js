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

const PLAYERS = [
  "alice",
  "bob",
  "charlie",
  "diana",
  "eve",
];

export default function () {
  client.connect("localhost:50051", { plaintext: true });

  // GetTop
  const topRes = client.invoke("leaderboard.v1.LeaderboardService/GetTop", {
    limit: 10,
  });
  check(topRes, {
    "GetTop status OK": (r) => r.status === grpc.StatusOK,
    "GetTop returns players": (r) => r.message.players !== undefined,
  });

  // GetPlayer — pick a random player from the pool
  const playerId = PLAYERS[Math.floor(Math.random() * PLAYERS.length)];
  const playerRes = client.invoke(
    "leaderboard.v1.LeaderboardService/GetPlayer",
    { player_id: playerId }
  );
  check(playerRes, {
    "GetPlayer status OK or NOT_FOUND": (r) =>
      r.status === grpc.StatusOK || r.status === grpc.StatusNotFound,
  });

  client.close();
  sleep(1);
}
