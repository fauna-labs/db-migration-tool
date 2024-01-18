class SchemaClient {
  #secret;
  #endpoint;

  constructor({ secret, endpoint }) {
    this.#secret = secret;
    this.#endpoint = endpoint;
  }

  async update(name, fsl) {
    try {
      const fd = new FormData();
      fd.append(name, Buffer.from(fsl));

      const url = new URL("/schema/1/update?force=true", this.#endpoint);

      // Just push.
      const res = await fetch(url, {
        method: "POST",
        headers: { AUTHORIZATION: `Bearer ${this.#secret}` },
        body: fd,
      });

      const json = await res.json();
      if (json.error) {
        throw new Error(json.error.message);
      }
    } catch (e) {
      throw new Error("Schema update failed", { cause: e });
    }
  }
}

module.exports = {
  SchemaClient,
};
