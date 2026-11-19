import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '5s', target: 20 },  // Ramp-up to 20 virtual users
    { duration: '15s', target: 50 }, // Sustained load at 50 VUs
    { duration: '5s', target: 0 },   // Ramp-down
  ],
  thresholds: {
    http_req_duration: ['p(95)<100'], // 95% of requests must complete below 100ms
    http_req_failed: ['rate<0.01'],    // Error rate must be less than 1%
  },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:3000';

export default function () {
  const payload = JSON.stringify({
    queueName: 'stress-test',
    jobType: 'transaction:verify',
    payload: {
      userId: `user-${__VU}-${__ITER}`,
      amount: Math.floor(Math.random() * 1000) + 1,
      currency: 'USD',
    },
    priority: Math.floor(Math.random() * 10),
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(`${BASE_URL}/api/jobs`, payload, params);

  check(res, {
    'status is 201': (r) => r.status === 201,
    'job ID returned': (r) => {
      const body = JSON.parse(r.body);
      return body.success === true && body.data.id !== undefined;
    },
  });

  sleep(0.05); // 50ms pause between requests per VU
}
