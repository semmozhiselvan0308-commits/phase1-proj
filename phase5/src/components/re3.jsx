import { useEffect, useMemo, useState } from "react";
import { RefreshCw } from "lucide-react";

import {
  MapContainer,
  TileLayer,
  GeoJSON,
  useMap,
} from "react-leaflet";

import { geoJSON as leafletGeoJSON } from "leaflet";

import "leaflet/dist/leaflet.css";
import "./re3.css";

const API =
  import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000";

const format = (value) =>
  Number(value).toLocaleString("en-US", {
    maximumFractionDigits: 1,
  });

/*
 * ---------------------------------------------------------
 * NORMALIZE GRID ID
 * ---------------------------------------------------------
 *
 * Handles:
 *   4821
 *   "4821"
 *   "4821.0"
 *   4821.0
 *
 * Also handles IDs where extra whitespace exists.
 */
const normalizeGridId = (value) => {
  if (value === null || value === undefined) {
    return "";
  }

  let id = String(value).trim();

  if (!id) {
    return "";
  }

  // Remove .0 from numeric IDs
  if (/^\d+\.0+$/.test(id)) {
    id = id.split(".")[0];
  }

  return id;
};


/*
 * =========================================================
 * RE3
 * =========================================================
 */

export default function Re3({ onNavigate }) {
  const [signals, setSignals] = useState([]);
  const [geo, setGeo] = useState(null);
  const [message, setMessage] = useState(
    "Loading operational signals..."
  );
  const [limit, setLimit] = useState("10");


  /*
   * -------------------------------------------------------
   * LOAD HOTSPOTS + ALERTS
   * -------------------------------------------------------
   */

  const load = async () => {
    try {
      setMessage("Loading operational signals...");

      const [hotspotsResponse, alertsResponse] =
        await Promise.all([
          fetch(
            `${API}/network/hotspots?limit=${limit}`
          ),
          fetch(
            `${API}/network/alerts?limit=${limit}`
          ),
        ]);

      if (
        !hotspotsResponse.ok ||
        !alertsResponse.ok
      ) {
        throw new Error(
          "Operational API is unavailable."
        );
      }

      const hotspotData =
        await hotspotsResponse.json();

      const alertData =
        await alertsResponse.json();

      const hotspotItems =
        hotspotData.items || [];

      const alertItems =
        alertData.items || [];


      /*
       * Keep both hotspot and alert records.
       */
      setSignals([
        ...hotspotItems,
        ...alertItems,
      ]);

      setMessage("");

      console.log(
        "RE3 hotspots:",
        hotspotItems
      );

      console.log(
        "RE3 alerts:",
        alertItems
      );

    } catch (error) {
      console.error(
        "Operational API error:",
        error
      );

      setMessage(
        error.message ||
          "Operational API is unavailable."
      );
    }
  };


  /*
   * Reload when limit changes.
   */

  useEffect(() => {
    void load();
  }, [limit]);


  /*
   * -------------------------------------------------------
   * LOAD MILAN GEOJSON
   * -------------------------------------------------------
   */

  useEffect(() => {
    const loadGeoJSON = async () => {
      try {
        const response = await fetch(
          "/reference/milano-grid.geojson"
        );

        if (!response.ok) {
          throw new Error(
            `GeoJSON request failed: ${response.status}`
          );
        }

        const data = await response.json();

        if (
          data?.type !== "FeatureCollection" ||
          !Array.isArray(data.features)
        ) {
          throw new Error(
            "Invalid Milan GeoJSON format."
          );
        }

        console.log(
          "Milan GeoJSON loaded"
        );

        console.log(
          "Grid cells:",
          data.features.length
        );

        setGeo(data);

      } catch (error) {
        console.error(
          "Milan GeoJSON error:",
          error
        );

        setMessage(
          "Milan GeoJSON could not be loaded."
        );
      }
    };

    void loadGeoJSON();
  }, []);


  /*
   * -------------------------------------------------------
   * BUILD ACTIVE GRID MAP
   * -------------------------------------------------------
   *
   * Example:
   *
   * 4821 -> attention
   * 48   -> high
   * 147  -> attention
   *
   * If a grid is both HIGH and ATTENTION,
   * HIGH wins.
   */

  const severity = useMemo(() => {
    const map = new Map();

    signals.forEach((item) => {
      const gridId = normalizeGridId(
        item.grid_id
      );

      if (!gridId) {
        return;
      }

      const apiSeverity =
        String(
          item.severity || ""
        ).toLowerCase();

      const existing =
        map.get(gridId);

      if (
        apiSeverity === "high" ||
        existing === "high"
      ) {
        map.set(
          gridId,
          "high"
        );
      } else {
        map.set(
          gridId,
          "attention"
        );
      }
    });

    return map;

  }, [signals]);


  /*
   * -------------------------------------------------------
   * FILTER GEOJSON
   * -------------------------------------------------------
   *
   * IMPORTANT:
   *
   * Original GeoJSON:
   *       10,000 cells
   *
   * API:
   *       hotspot + alert grid IDs
   *
   * Result:
   *       ONLY matching cells
   */

  const activeGeo = useMemo(() => {

    if (!geo) {
      return null;
    }

    const activeFeatures =
      geo.features.filter(
        (feature) => {

          const cellId =
            normalizeGridId(
              feature.properties?.cellId
            );

          return severity.has(
            cellId
          );
        }
      );

    console.log(
      "RE3 active GeoJSON cells:",
      activeFeatures.length
    );

    return {
      type: "FeatureCollection",
      features: activeFeatures,
    };

  }, [geo, severity]);


  /*
   * -------------------------------------------------------
   * ACTIVE CELL COUNT
   * -------------------------------------------------------
   */

  const activeCellCount =
    activeGeo?.features?.length || 0;


  /*
   * =======================================================
   * PAGE
   * =======================================================
   */

  return (
    <div className="page-body re3-page">

      {/* =================================================
          HEADER
         ================================================= */}

      <section className="page-intro">

        <div>

          <div className="eyebrow">
            <span className="eyebrow-line" />
            Risk signals / 03
          </div>

          <h1>
            Where the network
            <br />
            <em>needs a look.</em>
          </h1>

          <p className="intro-copy">
            Ranked hotspots, alerts, and geographic
            context across Milan.
          </p>

        </div>


        <div className="signal-controls">

          <label>
            Show top

            <select
              value={limit}
              onChange={(event) =>
                setLimit(event.target.value)
              }
            >
              <option value="10">
                10
              </option>

              <option value="20">
                20
              </option>

              <option value="50">
                50
              </option>
            </select>

          </label>


          <button
            type="button"
            onClick={load}
          >
            <RefreshCw size={15} />
            Refresh
          </button>

        </div>

      </section>


      {/* =================================================
          CONTENT
         ================================================= */}

      {signals.length > 0 ? (

        <section className="signal-layout">

          {/* =============================================
              PRIORITY QUEUE
             ============================================= */}

          <div className="panel data-panel">

            <div className="panel-heading">

              <div>

                <span className="section-kicker">
                  Priority queue
                </span>

                <h2>
                  Ranked attention areas
                </h2>

              </div>

              <span className="small-status">
                {signals.length} areas
              </span>

            </div>


            <div className="table-scroll">

              <table>

                <thead>

                  <tr>
                    <th>Grid</th>
                    <th>Activity</th>
                    <th>Status</th>
                    <th>Reason</th>
                  </tr>

                </thead>


                <tbody>

                  {signals.map(
                    (item, index) => {

                      const gridId =
                        normalizeGridId(
                          item.grid_id
                        );

                      const tone =
                        severity.get(
                          gridId
                        ) || "attention";


                      return (

                        <tr
                          key={`${gridId}-${item.timestamp}-${index}`}
                        >

                          <td>

                            <button
                              className="grid-link"
                              type="button"
                              onClick={() =>
                                onNavigate("re2")
                              }
                            >
                              {item.grid_id}
                            </button>

                          </td>


                          <td>
                            {format(
                              item.current_activity ??
                              item.total_activity ??
                              0
                            )}
                          </td>


                          <td>

                            <span
                              className={`status-badge ${tone}`}
                            >
                              {tone}
                            </span>

                          </td>


                          <td>
                            {item.reason ||
                              "Operational signal"}
                          </td>

                        </tr>

                      );
                    }
                  )}

                </tbody>

              </table>

            </div>

          </div>


          {/* =============================================
              MAP
             ============================================= */}

          <MilanMap
            geo={activeGeo}
            activeCellCount={
              activeCellCount
            }
            severity={severity}
            onSelect={() =>
              onNavigate("re2")
            }
          />

        </section>

      ) : (

        <div className="state-panel panel">
          {message}
        </div>

      )}

    </div>
  );
}


/*
 * =========================================================
 * MILAN MAP
 * =========================================================
 *
 * IMPORTANT:
 *
 * This receives activeGeo, NOT the complete GeoJSON.
 *
 * Therefore:
 *
 * NO normal cells are rendered.
 * ONLY hotspot/alert cells are rendered.
 */

function MilanMap({
  geo,
  activeCellCount,
  severity,
  onSelect,
}) {

  return (

    <div className="panel map-panel">

      {/* ===============================================
          MAP HEADER
         =============================================== */}

      <div className="panel-heading">

        <div>

          <span className="section-kicker">
            Geographic context
          </span>

          <h2>
            Milan grid
          </h2>

        </div>


        <span className="small-status">

          {geo
            ? `${activeCellCount} active cells`
            : "Loading"}

        </span>

      </div>


      {/* ===============================================
          MAP
         =============================================== */}

      <div className="map-stage">

        {!geo ? (

          <div className="map-loading">
            Loading Milan grid...
          </div>

        ) : (

          <MapContainer
            center={[
              45.4642,
              9.19,
            ]}
            zoom={11}
            scrollWheelZoom={true}
            zoomControl={true}
            style={{
              width: "100%",
              height: "100%",
            }}
          >

            {/* =========================================
                OPEN STREET MAP
               ========================================= */}

            <TileLayer
              attribution="&copy; OpenStreetMap contributors"
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />


            {/* =========================================
                FIT TO ACTIVE CELLS
               ========================================= */}

            <FitMilanBounds
              geo={geo}
            />


            {/* =========================================
                ONLY HOTSPOT / ALERT CELLS
               ========================================= */}

            <GeoJSON
              key={JSON.stringify(
                [...severity.entries()]
              )}
              data={geo}

              style={(feature) => {

                const cellId =
                  normalizeGridId(
                    feature.properties?.cellId
                  );

                const tone =
                  severity.get(
                    cellId
                  );


                /*
                 * HIGH
                 */

                if (
                  tone === "high"
                ) {

                  return {

                    color: "#b93820",

                    weight: 4,

                    opacity: 1,

                    fillColor:
                      "#e85f42",

                    fillOpacity:
                      0.9,

                  };

                }


                /*
                 * ATTENTION
                 */

                if (
                  tone === "attention"
                ) {

                  return {

                    color: "#9b7600",

                    weight: 4,

                    opacity: 1,

                    fillColor:
                      "#f2d35f",

                    fillOpacity:
                      0.9,

                  };

                }


                /*
                 * Safety fallback:
                 *
                 * This should never happen because
                 * activeGeo already filters the cells.
                 *
                 * Completely hide anything else.
                 */

                return {

                  opacity: 0,

                  fillOpacity: 0,

                  weight: 0,

                };

              }}


              onEachFeature={(
                feature,
                layer
              ) => {

                const cellId =
                  normalizeGridId(
                    feature.properties?.cellId
                  );

                const tone =
                  severity.get(
                    cellId
                  );


                /*
                 * Tooltip
                 */

                layer.bindTooltip(
                  `
                    Grid ${cellId}
                    <br/>
                    Status: ${
                      String(
                        tone || "attention"
                      ).toUpperCase()
                    }
                  `,
                  {
                    sticky: true,
                  }
                );


                /*
                 * Click
                 */

                layer.on(
                  "click",
                  () => {
                    onSelect();
                  }
                );


                /*
                 * Hover
                 */

                layer.on(
                  "mouseover",
                  (event) => {

                    event.target.setStyle({
                      weight: 6,
                      fillOpacity: 1,
                    });

                    event.target.bringToFront();
                  }
                );


                layer.on(
                  "mouseout",
                  (event) => {

                    if (
                      tone === "high"
                    ) {

                      event.target.setStyle({
                        weight: 4,
                        fillOpacity: 0.9,
                      });

                    } else {

                      event.target.setStyle({
                        weight: 4,
                        fillOpacity: 0.9,
                      });

                    }

                  }
                );

              }}

            />

          </MapContainer>

        )}

      </div>


      {/* ===============================================
          LEGEND
         =============================================== */}

      <div className="map-legend">

        <span>
          <i className="normal" />
          NORMAL
        </span>

        <span>
          <i className="attention" />
          ATTENTION
        </span>

        <span>
          <i className="high" />
          HIGH
        </span>

      </div>

    </div>
  );
}


/*
 * =========================================================
 * FIT MAP TO ACTIVE CELLS
 * =========================================================
 */

function FitMilanBounds({ geo }) {

  const map = useMap();

  useEffect(() => {

    if (
      !geo ||
      !geo.features ||
      geo.features.length === 0
    ) {
      return;
    }

    const layer =
      leafletGeoJSON(geo);

    const bounds =
      layer.getBounds();

    if (bounds.isValid()) {

      map.fitBounds(
        bounds,
        {
          padding: [
            50,
            50,
          ],
          maxZoom: 14,
        }
      );

    }

  }, [geo, map]);

  return null;
}