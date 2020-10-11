# test joined addres df
#address2_df.where(address_df.address == "12QtD5BFwRsdNsAZY76UVE1xyCGNTojH9h").show(truncate=False)


# addresses_df = ad_df.join(flagged_df, ad_df.address == flagged_df.address, "full").withColumn("address_coalesce",coalesce(ad_df.address, flagged_df.address))
# addresses_df = addresses_df.drop(flagged_df.address).drop(ad_df.address)


