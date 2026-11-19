import { Router } from "express";
import { listWorkers } from "../controllers/queues.controller.js";

const router = Router();

router.get("/", listWorkers);

export default router;
