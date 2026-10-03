const API = "https://scuibjobsai.onrender.com";
const BODY = {
  queries: ["frontend developer", "backend developer", "full stack developer", "python developer", "mobile developer", "devops engineer", "ai engineer", "software engineer", "data analyst", "product designer", "ui/ux designer", "graphics designer", "virtual assistant", "customer service", "sales representative"],
  locations: ["Nigeria"],
  sources: ["workable", "myjobmag", "hotnigerianjobs", "jobzilla", "delonjobs", "jsearch_api"],
  target_count: 60,
  remote_only: false,
  date_posted: "today",
  require_application_link: true,
  max_age_days: 1
};

addEventListener("fetch", (event) => {
  event.respondWith(new Response("SCUIB Jobs cron worker (6-hourly)"));
});

addEventListener("scheduled", (event) => {
  event.waitUntil(runCron());
});

async function runCron() {
  try {
    const resp = await fetch(API + "/ingest/bulk", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(BODY)
    });
    const data = await resp.json().catch(() => ({}));
    console.log("bulk status:", resp.status, "run_id:", data.run_id);
    if (!resp.ok || !data.run_id) {
      console.error("scheduling failed:", resp.status, JSON.stringify(data));
      return;
    }
    const deadline = Date.now() + 8 * 60 * 1000;
    let last = null;
    while (Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 20000));
      const s = await fetch(API + "/ingest/runs/" + data.run_id + "/status");
      const st = await s.json().catch(() => ({}));
      last = st;
      console.log("run status:", JSON.stringify(st));
      if (st.state === "completed" || st.state === "failed") break;
    }
    console.log("final:", JSON.stringify(last));
  } catch (e) {
    console.error("cron failed:", (e && e.message) || e);
  }
}
