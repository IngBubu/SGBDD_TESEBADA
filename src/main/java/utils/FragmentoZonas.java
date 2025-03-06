package utils;

import java.util.HashMap;
import java.util.Map;

public class FragmentoZonas {
    private static final Map<String, String> mapaEstadosZona = new HashMap<>();

    static {
        // 🔹 Mapeo de estados a zonas (ahora en minúsculas)
        mapaEstadosZona.put("durango", "ZonaNorte");
        mapaEstadosZona.put("baja california sur", "ZonaNorte");
        mapaEstadosZona.put("chihuahua", "ZonaNorte");
        mapaEstadosZona.put("sinaloa", "ZonaNorte");
        mapaEstadosZona.put("baja california", "ZonaNorte");
        mapaEstadosZona.put("coahuila", "ZonaNorte");
        mapaEstadosZona.put("tamaulipas", "ZonaNorte");
        mapaEstadosZona.put("sonora", "ZonaNorte");

        mapaEstadosZona.put("zacatecas", "ZonaCentro");
        mapaEstadosZona.put("hidalgo", "ZonaCentro");
        mapaEstadosZona.put("queretaro", "ZonaCentro");
        mapaEstadosZona.put("guanajuato", "ZonaCentro");
        mapaEstadosZona.put("Tlaxcala", "ZonaCentro");
        mapaEstadosZona.put("cdmx", "ZonaCentro");
        mapaEstadosZona.put("morelos", "ZonaCentro");
        mapaEstadosZona.put("aguascalientes", "ZonaCentro");
        mapaEstadosZona.put("san luis potosi", "ZonaCentro");
        mapaEstadosZona.put("michoacan", "ZonaCentro");

        mapaEstadosZona.put("oaxaca", "ZonaSur");
        mapaEstadosZona.put("veracruz", "ZonaSur");
        mapaEstadosZona.put("guerrero", "ZonaSur");
        mapaEstadosZona.put("quintana roo", "ZonaSur");
        mapaEstadosZona.put("campeche", "ZonaSur");
        mapaEstadosZona.put("tabasco", "ZonaSur");
        mapaEstadosZona.put("chiapas", "ZonaSur");
    }

    public static String obtenerZonaPorEstado(String estado) {
        return mapaEstadosZona.getOrDefault(estado.toLowerCase(), "ZonaDesconocida");
    }
}
