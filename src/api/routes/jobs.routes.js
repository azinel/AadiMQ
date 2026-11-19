import { Router } from "express";
import { createJob, getJobById, listJobs, cancelJob, retryJob } from "../controllers/jobs.controller.js";
import { validateBody } from "../middlewares/validator.js";
import { EnqueueJobSchema } from "../../queue/producer.js";

const router = Router();

router.post("/", validateBody(EnqueueJobSchema), createJob);
router.get("/:id", getJobById);
router.get("/", listJobs);
router.delete("/:id", cancelJob);
router.post("/:id/retry", retryJob);

export default router;
