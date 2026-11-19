import http from 'k6/http';
import { check, group, sleep } from 'k6';

export const options = {
  vus: 30,
  duration: '15s',
  thresholds: {
    http_req_duration: ['p(95)<50'], // High-speed read queries p95 < 50ms
    http_req_failed: ['rate<0.01'],
  },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:3000';

export default function () {
  group('Health & Stats Read Queries', () => {
    // 1. Cluster Health Query
    const healthRes = http.get(`${BASE_URL}/api/health`);
    check(healthRes, {
      'health status 200': (r) => r.status === 200,
    });

    // 2. Queue Aggregates Query
    const queuesRes = http.get(`${BASE_URL}/api/queues`);
    check(queuesRes, {
      'queues status 200': (r) => r.status === 200,
    });

    // 3. Worker Fleet Query
    const workersRes = http.get(`${BASE_URL}/api/workers`);
    check(workersRes, {
      'workers status 200': (r) => r.status === 200,
    });
  });

  sleep(0.1);
}
