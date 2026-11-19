import { Router } from "express";
import { listQueues, getQueueStats } from "../controllers/queues.controller.js";

const router = Router();

router.get("/", listQueues);
router.get("/:name/stats", getQueueStats);

export default router;
