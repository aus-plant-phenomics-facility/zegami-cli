SELECT
(regexp_replace(COALESCE(snapshot.id_tag, '0'), '[^0-9]+', '', 'g') || floor(extract(epoch 
from
   time_stamp)))::bigint AS "id",
   snapshot.id_tag AS "Plant ID",
   {metadata_view_fields}
   trim(both '"' from to_json(snapshot.time_stamp)::text) as "Time",
   DATE_PART('day', snapshot.time_stamp - '{imaging_day}') as "Imaging Day",
   snapshot.water_amount AS "Water Amount",
   snapshot.weight_after AS "Weight After",
   snapshot.weight_before AS "Weight Before",
   (rgb_side_far_0_analysis."RGB_Side_Far_0 Area" + rgb_side_far_4_analysis."RGB_Side_Far_4 Area" + rgb_side_lower_0_analysis."RGB_Side_Lower_0 Area" + rgb_side_lower_4_analysis."RGB_Side_Lower_4 Area" + rgb_side_upper_0_analysis."RGB_Side_Upper_0 Area" + rgb_side_upper_4_analysis."RGB_Side_Upper_4 Area" + rgb_tv_analysis."RGB_TV Area")::real AS "Projected Shoot Area",
   (rgb_side_far_0_analysis."RGB_Side_Far_0 Area" + rgb_side_far_4_analysis."RGB_Side_Far_4 Area")::real AS "Side Far Projected Shoot Area",
   (rgb_side_lower_0_analysis."RGB_Side_Lower_0 Area" + rgb_side_lower_4_analysis."RGB_Side_Lower_4 Area")::real AS "Side Lower Projected Shoot Area",
   (rgb_side_upper_0_analysis."RGB_Side_Upper_0 Area" + rgb_side_upper_4_analysis."RGB_Side_Upper_4 Area")::real AS "Side Upper Projected Shoot Area",
   substring(path_view."RGB_3D_3D_side_far_0" FROM '[\d-]+/(.*)') as "RGB_3D_3D_side_far_0",
   path_view."RGB_3D_3D_side_far_0" as "RGB_3D_3D_side_far_0_path"
FROM
   snapshot 
   LEFT JOIN
      metadata_view 
      ON snapshot.id_tag = metadata_view.id_tag 
   RIGHT JOIN
      path_view 
      ON path_view.snapshot_id = snapshot.id 
   LEFT JOIN
      rgb_side_far_0_analysis 
      ON rgb_side_far_0_analysis."RGB_Side_Far_0_snapshot_id" = snapshot.id 
   LEFT JOIN
      rgb_side_far_4_analysis 
      ON rgb_side_far_4_analysis."RGB_Side_Far_4_snapshot_id" = snapshot.id 
   LEFT JOIN
      rgb_side_lower_0_analysis 
      ON rgb_side_lower_0_analysis."RGB_Side_Lower_0_snapshot_id" = snapshot.id 
   LEFT JOIN
      rgb_side_lower_4_analysis 
      ON rgb_side_lower_4_analysis."RGB_Side_Lower_4_snapshot_id" = snapshot.id 
   LEFT JOIN
      rgb_side_upper_0_analysis 
      ON rgb_side_upper_0_analysis."RGB_Side_Upper_0_snapshot_id" = snapshot.id 
   LEFT JOIN
      rgb_side_upper_4_analysis 
      ON rgb_side_upper_4_analysis."RGB_Side_Upper_4_snapshot_id" = snapshot.id 
   LEFT JOIN
      rgb_tv_analysis 
      ON rgb_tv_analysis."RGB_TV_snapshot_id" = snapshot.id 
WHERE 
   snapshot.measurement_label = '{measurement_label}' 
ORDER BY 
   "Plant ID", 
   time_stamp 
