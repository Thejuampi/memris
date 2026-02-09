(() => {
  const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  const reveals = document.querySelectorAll(".reveal");

  if (reduceMotion) {
    reveals.forEach((el) => el.classList.add("show"));
  } else if ("IntersectionObserver" in window) {
    const observer = new IntersectionObserver(
      (entries, obs) => {
        entries.forEach((entry) => {
          if (!entry.isIntersecting) {
            return;
          }
          entry.target.classList.add("show");
          obs.unobserve(entry.target);
        });
      },
      { threshold: 0.16 }
    );

    reveals.forEach((el) => observer.observe(el));
  } else {
    reveals.forEach((el) => el.classList.add("show"));
  }

  const sectionIds = ["why", "architecture", "query", "concurrency", "roadmap"];
  const navLinks = [...document.querySelectorAll(".nav-list a")];
  const sectionMap = new Map(sectionIds.map((id) => [id, document.getElementById(id)]));

  const setActive = (activeId) => {
    navLinks.forEach((link) => {
      const id = link.getAttribute("href").replace("#", "");
      link.classList.toggle("active", id === activeId);
    });
  };

  const updateActiveSection = () => {
    let activeId = "";
    let bestTop = Number.POSITIVE_INFINITY;

    sectionMap.forEach((section, id) => {
      if (!section) {
        return;
      }
      const top = section.getBoundingClientRect().top;
      if (top >= 0 && top < bestTop) {
        bestTop = top;
        activeId = id;
      }
    });

    if (!activeId) {
      for (const id of sectionIds) {
        const section = sectionMap.get(id);
        if (!section) {
          continue;
        }
        if (section.getBoundingClientRect().top < 0) {
          activeId = id;
        }
      }
    }

    setActive(activeId);
  };

  document.addEventListener("scroll", updateActiveSection, { passive: true });
  updateActiveSection();
})();
